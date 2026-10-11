//! Ingest pipelines: `_ingest/pipeline` CRUD, `_ingest/pipeline/_simulate`,
//! `_ingest/_simulate`, and running pipelines on writes (`?pipeline=`,
//! a bulk item's `pipeline`, `index.default_pipeline` and
//! `index.final_pipeline`).
//!
//! A document runs through its pipelines as a map of its source fields
//! plus `_index`, `_id` and `_routing` (the `ctx` scripts see), with the
//! `_ingest` metadata beside it. The common ingest-common processors are
//! implemented; an unknown processor type is refused when the pipeline is
//! stored, as Elasticsearch does.

use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};

use super::{Engine, State, error, glob_match, painless, parse_json};

/// An ingest failure: Elasticsearch's exception type, reason and status
/// (boxed: errors travel through every processor's `Result`).
#[derive(Debug, Clone)]
pub(super) struct IngestError(Box<ErrorInner>);

#[derive(Debug, Clone)]
pub(super) struct ErrorInner {
    kind: String,
    reason: String,
    status: u16,
    caused_by: Option<Value>,
    extra: Map<String, Value>,
    /// The processor (type, tag) that failed, for `on_failure` metadata.
    failed: Option<(String, String)>,
}

impl std::ops::Deref for IngestError {
    type Target = ErrorInner;
    fn deref(&self) -> &ErrorInner {
        &self.0
    }
}

impl std::ops::DerefMut for IngestError {
    fn deref_mut(&mut self) -> &mut ErrorInner {
        &mut self.0
    }
}

impl IngestError {
    fn new(kind: &str, reason: impl Into<String>, status: u16) -> Self {
        Self(Box::new(ErrorInner {
            kind: kind.into(),
            reason: reason.into(),
            status,
            caused_by: None,
            extra: Map::new(),
            failed: None,
        }))
    }

    fn iae(reason: impl Into<String>) -> Self {
        Self::new("illegal_argument_exception", reason, 400)
    }

    fn parse(reason: impl Into<String>, processor: Option<&str>, property: Option<&str>) -> Self {
        let mut e = Self::new("parse_exception", reason, 400);
        if let Some(p) = property {
            e.extra.insert("property_name".into(), json!(p));
        }
        if let Some(p) = processor {
            e.extra.insert("processor_type".into(), json!(p));
        }
        e
    }

    fn caused(mut self, kind: &str, reason: &str) -> Self {
        self.caused_by = Some(json!({"type": kind, "reason": reason}));
        self
    }

    /// `{type, reason, ...}`.
    fn body(&self) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("type".into(), json!(self.kind));
        m.insert("reason".into(), json!(self.reason));
        m.extend(self.extra.clone());
        m
    }

    /// `{root_cause: [...], type, reason, caused_by}`.
    fn error_object(&self) -> Value {
        let mut m = self.body();
        if let Some(c) = &self.caused_by {
            m.insert("caused_by".into(), c.clone());
        }
        let mut out = Map::new();
        out.insert("root_cause".into(), json!([Value::Object(self.body())]));
        out.extend(m);
        Value::Object(out)
    }

    pub(super) fn response(&self) -> (u16, Value) {
        (self.status, json!({"error": self.error_object(), "status": self.status}))
    }
}

type R<T> = Result<T, IngestError>;

// ---------------------------------------------------------------------
// Documents and field paths
// ---------------------------------------------------------------------

/// A document going through a pipeline.
#[derive(Clone, Debug)]
pub(super) struct Doc {
    /// Source fields plus `_index`, `_id`, `_routing`.
    ctx: Map<String, Value>,
    /// `_ingest` metadata (`timestamp`, `pipeline`, `on_failure_*`, ...).
    ingest: Map<String, Value>,
}

const META: &[&str] =
    &["_index", "_id", "_routing", "_version", "_version_type", "_if_seq_no", "_if_primary_term"];

impl Doc {
    fn new(
        index: &str,
        id: Option<&str>,
        routing: Option<&str>,
        source: Map<String, Value>,
    ) -> Self {
        let mut ctx = source;
        ctx.insert("_index".into(), json!(index));
        if let Some(id) = id {
            ctx.insert("_id".into(), json!(id));
        }
        if let Some(r) = routing {
            ctx.insert("_routing".into(), json!(r));
        }
        let mut ingest = Map::new();
        ingest.insert(
            "timestamp".into(),
            json!(super::super::dates::format(super::super::dates::now_ms(), None, 0)),
        );
        Self { ctx, ingest }
    }

    fn meta(&self, k: &str) -> Option<String> {
        match self.ctx.get(k)? {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            other => Some(other.to_string()),
        }
    }

    fn source(&self) -> Map<String, Value> {
        self.ctx
            .iter()
            .filter(|(k, _)| !META.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// The map a template or script sees: source, metadata and `_ingest`.
    fn view(&self) -> Value {
        let mut m = self.ctx.clone();
        m.insert("_ingest".into(), Value::Object(self.ingest.clone()));
        m.insert("_source".into(), Value::Object(self.source()));
        Value::Object(m)
    }

    /// The map and the path inside it a field name refers to.
    fn root<'a>(&mut self, path: &'a str) -> (&mut Map<String, Value>, &'a str) {
        if let Some(rest) = path.strip_prefix("_ingest.") {
            (&mut self.ingest, rest)
        } else if let Some(rest) = path.strip_prefix("_source.") {
            (&mut self.ctx, rest)
        } else {
            (&mut self.ctx, path)
        }
    }

    fn get(&mut self, path: &str) -> R<Value> {
        let full = path.to_string();
        let (root, p) = self.root(path);
        get_path(root, p, &full).cloned()
    }

    fn get_opt(&mut self, path: &str) -> Option<Value> {
        let (root, p) = self.root(path);
        get_path(root, p, path).ok().cloned()
    }

    fn has(&mut self, path: &str) -> bool {
        let (root, p) = self.root(path);
        get_path(root, p, path).is_ok()
    }

    fn set(&mut self, path: &str, v: Value) -> R<()> {
        let full = path.to_string();
        let (root, p) = self.root(path);
        set_path(root, p, v, &full)
    }

    fn remove(&mut self, path: &str) -> R<Value> {
        let full = path.to_string();
        let (root, p) = self.root(path);
        remove_path(root, p, &full)
    }
}

/// Java's class name for a JSON value, as error messages print it.
fn java_type(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "java.lang.String",
        Value::Bool(_) => "java.lang.Boolean",
        Value::Number(n) if n.is_f64() => "java.lang.Double",
        Value::Number(n) if n.as_i64().is_some_and(|x| i32::try_from(x).is_ok()) => {
            "java.lang.Integer"
        }
        Value::Number(_) => "java.lang.Long",
        Value::Array(_) => "java.util.ArrayList",
        Value::Object(_) => "java.util.HashMap",
        Value::Null => "null",
    }
}

fn get_path<'a>(root: &'a Map<String, Value>, path: &str, full: &str) -> R<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut cur: Option<&Value> = None;
    for p in &parts {
        let next = match cur {
            None => root.get(*p),
            Some(Value::Object(m)) => m.get(*p),
            Some(Value::Array(a)) => {
                let Ok(k) = p.parse::<usize>() else {
                    return Err(IngestError::iae(format!(
                        "[{p}] is not an integer, cannot be used as an index as part of path [{full}]"
                    )));
                };
                if k >= a.len() {
                    return Err(IngestError::iae(format!(
                        "[{p}] is out of bounds for array with length [{}] as part of path [{full}]",
                        a.len()
                    )));
                }
                a.get(k)
            }
            Some(other) => {
                return Err(IngestError::iae(format!(
                    "cannot resolve [{p}] from object of type [{}] as part of path [{full}]",
                    java_type(other)
                )));
            }
        };
        match next {
            Some(v) => cur = Some(v),
            None => {
                return Err(IngestError::iae(format!(
                    "field [{p}] not present as part of path [{full}]"
                )));
            }
        }
    }
    cur.ok_or_else(|| {
        IngestError::iae(format!("field [{path}] not present as part of path [{full}]"))
    })
}

fn set_path(root: &mut Map<String, Value>, path: &str, v: Value, full: &str) -> R<()> {
    let parts: Vec<&str> = path.split('.').collect();
    set_in_map(root, &parts, v, full)
}

fn set_in_map(m: &mut Map<String, Value>, parts: &[&str], v: Value, full: &str) -> R<()> {
    let (first, rest) = parts.split_first().expect("a path has a part");
    if rest.is_empty() {
        m.insert(first.to_string(), v);
        return Ok(());
    }
    let child = m.entry(first.to_string()).or_insert_with(|| json!({}));
    if child.is_null() {
        *child = json!({});
    }
    set_in_value(child, rest, v, full)
}

fn set_in_value(cur: &mut Value, parts: &[&str], v: Value, full: &str) -> R<()> {
    match cur {
        Value::Object(m) => set_in_map(m, parts, v, full),
        Value::Array(a) => {
            let p = parts[0];
            let Some(k) = p.parse::<usize>().ok().filter(|k| *k < a.len()) else {
                return Err(IngestError::iae(format!(
                    "[{p}] is not an integer, cannot be used as an index as part of path [{full}]"
                )));
            };
            if parts.len() == 1 {
                a[k] = v;
                Ok(())
            } else {
                set_in_value(&mut a[k], &parts[1..], v, full)
            }
        }
        other => Err(IngestError::iae(format!(
            "cannot set [{}] with parent object of type [{}] as part of path [{full}]",
            parts[0],
            java_type(other)
        ))),
    }
}

fn remove_path(root: &mut Map<String, Value>, path: &str, full: &str) -> R<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let (last, init) = parts.split_last().unwrap();
    if init.is_empty() {
        return root.remove(*last).ok_or_else(|| {
            IngestError::iae(format!("field [{last}] not present as part of path [{full}]"))
        });
    }
    let mut cur = root.get_mut(init[0]).ok_or_else(|| {
        IngestError::iae(format!("field [{}] not present as part of path [{full}]", init[0]))
    })?;
    for p in &init[1..] {
        cur = match cur {
            Value::Object(m) => m.get_mut(*p).ok_or_else(|| {
                IngestError::iae(format!("field [{p}] not present as part of path [{full}]"))
            })?,
            Value::Array(a) => {
                let k = p.parse::<usize>().ok().filter(|k| *k < a.len()).ok_or_else(|| {
                    IngestError::iae(format!("[{p}] is out of bounds as part of path [{full}]"))
                })?;
                &mut a[k]
            }
            other => {
                return Err(IngestError::iae(format!(
                    "cannot resolve [{p}] from object of type [{}] as part of path [{full}]",
                    java_type(other)
                )));
            }
        };
    }
    match cur {
        Value::Object(m) => m.remove(*last).ok_or_else(|| {
            IngestError::iae(format!("field [{last}] not present as part of path [{full}]"))
        }),
        Value::Array(a) => match last.parse::<usize>() {
            Ok(k) if k < a.len() => Ok(a.remove(k)),
            _ => {
                Err(IngestError::iae(format!("[{last}] is out of bounds as part of path [{full}]")))
            }
        },
        other => Err(IngestError::iae(format!(
            "cannot remove [{last}] from object of type [{}] as part of path [{full}]",
            java_type(other)
        ))),
    }
}

/// A value as Java's `toString` prints it (templates render this way).
fn java_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Array(a) => {
            format!("[{}]", a.iter().map(java_string).collect::<Vec<_>>().join(", "))
        }
        Value::Object(m) => {
            format!(
                "{{{}}}",
                m.iter()
                    .map(|(k, x)| format!("{k}={}", java_string(x)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().unwrap_or(0.0);
            if f.fract() == 0.0 && f.abs() < 1e7 { format!("{f:.1}") } else { f.to_string() }
        }
        other => other.to_string(),
    }
}

/// Renders `{{field}}` / `{{{field}}}` mustache references.
fn render(template: &str, doc: &Doc) -> String {
    if !template.contains("{{") {
        return template.to_string();
    }
    let view = doc.view();
    let mut out = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let triple = rest[start..].starts_with("{{{");
        let open = if triple { 3 } else { 2 };
        let close = if triple { "}}}" } else { "}}" };
        let Some(end) = rest[start + open..].find(close) else {
            out.push_str(&rest[start..]);
            return out;
        };
        let name = rest[start + open..start + open + end].trim();
        let mut cur = Some(&view);
        for p in name.split('.') {
            cur = cur.and_then(|c| match c {
                Value::Object(m) => m.get(p),
                Value::Array(a) => p.parse::<usize>().ok().and_then(|k| a.get(k)),
                _ => None,
            });
        }
        if let Some(v) = cur {
            out.push_str(&java_string(v));
        }
        rest = &rest[start + open + end + close.len()..];
    }
    out.push_str(rest);
    out
}

/// A configured value with its string parts rendered as templates.
fn render_value(v: &Value, doc: &Doc) -> Value {
    match v {
        Value::String(s) => json!(render(s, doc)),
        Value::Array(a) => Value::Array(a.iter().map(|x| render_value(x, doc)).collect()),
        Value::Object(m) => {
            Value::Object(m.iter().map(|(k, x)| (render(k, doc), render_value(x, doc))).collect())
        }
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------
// Processor configuration
// ---------------------------------------------------------------------

/// Options every processor takes.
const COMMON: &[&str] = &["if", "ignore_failure", "on_failure", "tag", "description"];

/// Each processor type: (required options, optional options).
fn processor_spec(kind: &str) -> Option<(&'static [&'static str], &'static [&'static str])> {
    Some(match kind {
        "set" => {
            (&["field"], &["value", "copy_from", "override", "ignore_empty_value", "media_type"])
        }
        "remove" => (&[], &["field", "ignore_missing", "keep"]),
        "rename" => (&["field", "target_field"], &["ignore_missing", "override"]),
        "lowercase" | "uppercase" | "trim" | "urldecode" | "html_strip" | "bytes" => {
            (&["field"], &["target_field", "ignore_missing"])
        }
        "append" => (&["field", "value"], &["allow_duplicates", "media_type"]),
        "convert" => (&["field", "type"], &["target_field", "ignore_missing"]),
        "date" => (&["field", "formats"], &["target_field", "timezone", "locale", "output_format"]),
        "split" => {
            (&["field", "separator"], &["target_field", "ignore_missing", "preserve_trailing"])
        }
        "join" => (&["field", "separator"], &["target_field"]),
        "gsub" => (&["field", "pattern", "replacement"], &["target_field", "ignore_missing"]),
        "grok" => (
            &["field", "patterns"],
            &["pattern_definitions", "ignore_missing", "trace_match", "ecs_compatibility"],
        ),
        "dissect" => (&["field", "pattern"], &["append_separator", "ignore_missing"]),
        "script" => (&[], &["source", "id", "params", "lang", "inline", "options"]),
        "pipeline" => (&["name"], &["ignore_missing_pipeline"]),
        "fail" => (&["message"], &[]),
        "drop" => (&[], &[]),
        "foreach" => (&["field", "processor"], &["ignore_missing"]),
        "json" => (
            &["field"],
            &[
                "target_field",
                "add_to_root",
                "add_to_root_conflict_strategy",
                "allow_duplicate_keys",
                "strict_json_parsing",
            ],
        ),
        "kv" => (
            &["field", "field_split", "value_split"],
            &[
                "target_field",
                "include_keys",
                "exclude_keys",
                "ignore_missing",
                "prefix",
                "trim_key",
                "trim_value",
                "strip_brackets",
            ],
        ),
        "dot_expander" => (&["field"], &["path", "override"]),
        "sort" => (&["field"], &["order", "target_field"]),
        "reroute" => (&[], &["destination", "dataset", "namespace"]),
        "csv" => (
            &["field", "target_fields"],
            &["separator", "quote", "ignore_missing", "trim", "empty_value"],
        ),
        _ => return None,
    })
}

/// Checks a pipeline definition the way `PUT _ingest/pipeline` does.
fn validate_pipeline(id: &str, p: &Value) -> R<()> {
    let Some(m) = p.as_object() else {
        return Err(IngestError::parse("pipeline must be an object", None, None));
    };
    let unknown: Vec<&String> = m
        .keys()
        .filter(|k| {
            !matches!(
                k.as_str(),
                "description" | "processors" | "on_failure" | "version" | "_meta" | "deprecated"
            )
        })
        .collect();
    if !unknown.is_empty() {
        let list = unknown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
        return Err(IngestError::parse(
            format!(
                "pipeline [{id}] doesn't support one or more provided configuration parameters [{list}]"
            ),
            None,
            None,
        ));
    }
    let Some(procs) = m.get("processors") else {
        return Err(IngestError::parse(
            "[processors] required property is missing",
            None,
            Some("processors"),
        ));
    };
    validate_processor_list(procs, "processors")?;
    if let Some(of) = m.get("on_failure") {
        validate_processor_list(of, "on_failure")?;
        if of.as_array().is_some_and(Vec::is_empty) {
            return Err(IngestError::parse(
                "pipeline [".to_string() + id + "] cannot have an empty on-failure option defined",
                None,
                None,
            ));
        }
    }
    if let Some(v) = m.get("version")
        && !v.is_i64()
    {
        return Err(IngestError::parse(
            format!("[version] property isn't an integer, but of type [{}]", java_type(v)),
            None,
            Some("version"),
        ));
    }
    Ok(())
}

fn validate_processor_list(v: &Value, name: &str) -> R<()> {
    let Some(list) = v.as_array() else {
        return Err(IngestError::parse(
            format!("[{name}] property isn't a list, but of type [{}]", java_type(v)),
            None,
            Some(name),
        ));
    };
    for p in list {
        validate_processor(p)?;
    }
    Ok(())
}

fn validate_processor(p: &Value) -> R<()> {
    let Some((kind, cfg)) = p.as_object().filter(|m| m.len() == 1).and_then(|m| m.iter().next())
    else {
        return Err(IngestError::parse(
            "processor must be an object with a single key",
            None,
            None,
        ));
    };
    let Some((required, optional)) = processor_spec(kind) else {
        return Err(IngestError::parse(
            format!("No processor type exists with name [{kind}]"),
            Some(kind),
            None,
        ));
    };
    let Some(cfg) = cfg.as_object() else {
        return Err(IngestError::parse(
            format!("processor [{kind}] must be an object"),
            Some(kind),
            None,
        ));
    };
    for r in required {
        if !cfg.contains_key(*r) {
            return Err(IngestError::parse(
                format!("[{r}] required property is missing"),
                Some(kind),
                Some(r),
            ));
        }
    }
    match kind.as_str() {
        "set" if !cfg.contains_key("value") && !cfg.contains_key("copy_from") => {
            return Err(IngestError::parse(
                "[value] required property is missing",
                Some(kind),
                Some("value"),
            ));
        }
        "set" if cfg.contains_key("value") && cfg.contains_key("copy_from") => {
            return Err(IngestError::parse(
                "[copy_from] cannot set both `copy_from` and `value` in the same processor",
                Some(kind),
                Some("copy_from"),
            ));
        }
        "remove" if !cfg.contains_key("field") && !cfg.contains_key("keep") => {
            return Err(IngestError::parse(
                "[keep] or [field] must be specified",
                Some(kind),
                Some("keep"),
            ));
        }
        "script"
            if !cfg.contains_key("source")
                && !cfg.contains_key("id")
                && !cfg.contains_key("inline") =>
        {
            return Err(IngestError::parse(
                "must specify either [source] for an inline script or [id] for a stored script",
                Some(kind),
                None,
            ));
        }
        "convert" => {
            let t = cfg.get("type").and_then(Value::as_str).unwrap_or("");
            if !matches!(
                t,
                "integer" | "long" | "float" | "double" | "boolean" | "string" | "auto" | "ip"
            ) {
                return Err(IngestError::parse(
                    format!("type [{t}] not supported, cannot convert field."),
                    Some(kind),
                    Some("type"),
                ));
            }
        }
        "foreach" => {
            if let Some(inner) = cfg.get("processor") {
                validate_processor(inner)?;
            }
        }
        _ => {}
    }
    let unknown: Vec<&String> = cfg
        .keys()
        .filter(|k| {
            !required.contains(&k.as_str())
                && !optional.contains(&k.as_str())
                && !COMMON.contains(&k.as_str())
        })
        .collect();
    if !unknown.is_empty() {
        let list = unknown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
        return Err(IngestError::parse(
            format!(
                "processor [{kind}] doesn't support one or more provided configuration parameters [{list}]"
            ),
            Some(kind),
            None,
        ));
    }
    if let Some(of) = cfg.get("on_failure") {
        validate_processor_list(of, "on_failure")?;
    }
    if let Some(cond) = cfg.get("if") {
        let src = match cond {
            Value::String(s) => s.clone(),
            other => painless::script_parts(other).map(|x| x.0).unwrap_or_default(),
        };
        if let Err(e) = painless::compile(&src) {
            let mut err = IngestError::new("script_exception", "compile error", 400);
            err.extra.insert("script".into(), json!(src));
            err.extra.insert("lang".into(), json!("painless"));
            return Err(err.caused("illegal_argument_exception", &e));
        }
    }
    if kind == "script"
        && let Some(src) = cfg.get("source").or_else(|| cfg.get("inline")).and_then(Value::as_str)
        && let Err(e) = painless::compile(src)
    {
        let mut err = IngestError::new("script_exception", "compile error", 400);
        err.extra.insert("script".into(), json!(src));
        err.extra.insert("lang".into(), json!("painless"));
        return Err(err.caused("illegal_argument_exception", &e));
    }
    if kind == "grok"
        && let Err(e) = Grok::new(cfg)
    {
        return Err(IngestError::parse(e.reason.clone(), Some("grok"), None));
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Running pipelines
// ---------------------------------------------------------------------

/// Where pipelines are looked up: the stored ones, with simulate's
/// substitutions taking precedence.
struct Store<'a> {
    pipelines: &'a BTreeMap<String, Value>,
    substitutions: Option<&'a Map<String, Value>>,
    scripts: &'a BTreeMap<String, Value>,
}

impl Store<'_> {
    /// Pipeline `id`; looking any pipeline up parses every substitution
    /// first, as Elasticsearch does.
    fn get(&self, id: &str) -> R<Option<Value>> {
        if let Some(subs) = self.substitutions {
            for (sid, p) in subs {
                if let Err(e) = validate_pipeline(sid, p) {
                    let mut err = IngestError::new(
                        "runtime_exception",
                        format!("org.elasticsearch.ElasticsearchParseException: {}", e.reason),
                        500,
                    );
                    let mut cause = e.body();
                    cause.remove("root_cause");
                    err.caused_by = Some(Value::Object(cause));
                    return Err(err);
                }
            }
            if let Some(p) = subs.get(id) {
                return Ok(Some(p.clone()));
            }
        }
        Ok(self.pipelines.get(id).cloned())
    }
}

enum Flow {
    Continue,
    Drop,
}

/// One processor's outcome, for `?verbose` simulations.
struct Trace {
    results: Vec<Value>,
    on: bool,
}

impl Trace {
    fn push(&mut self, v: Value) {
        if self.on {
            self.results.push(v);
        }
    }
}

struct Runner<'a> {
    store: &'a Store<'a>,
    /// Pipelines being run (cycle detection).
    stack: Vec<String>,
    trace: Trace,
}

fn processor_parts(p: &Value) -> (&str, &Map<String, Value>) {
    static EMPTY: std::sync::OnceLock<Map<String, Value>> = std::sync::OnceLock::new();
    match p.as_object().and_then(|m| m.iter().next()) {
        Some((k, Value::Object(c))) => (k.as_str(), c),
        Some((k, _)) => (k.as_str(), EMPTY.get_or_init(Map::new)),
        None => ("", EMPTY.get_or_init(Map::new)),
    }
}

fn opt_bool(cfg: &Map<String, Value>, k: &str, default: bool) -> bool {
    match cfg.get(k) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => default,
    }
}

fn opt_str<'a>(cfg: &'a Map<String, Value>, k: &str) -> Option<&'a str> {
    cfg.get(k).and_then(Value::as_str)
}

impl Runner<'_> {
    /// Runs pipeline `id` over `doc`.
    fn pipeline(&mut self, id: &str, doc: &mut Doc) -> R<Flow> {
        let Some(p) = self.store.get(id)? else {
            return Err(IngestError::iae(format!("pipeline with id [{id}] does not exist")));
        };
        if self.stack.iter().any(|x| x == id) {
            return Err(IngestError::new(
                "graph_structure_exception",
                format!("Cycle detected for pipeline: {id}"),
                400,
            ));
        }
        self.stack.push(id.to_string());
        let prev = doc.ingest.insert("pipeline".into(), json!(id));
        let procs = p.get("processors").and_then(Value::as_array).cloned().unwrap_or_default();
        let result = match self.processors(&procs, doc) {
            Err(e) => match p.get("on_failure").and_then(Value::as_array) {
                Some(handlers) if !handlers.is_empty() => {
                    let handlers = handlers.clone();
                    let (kind, tag) = e.failed.clone().unwrap_or_default();
                    self.on_failure(&handlers, doc, &e, &kind, &tag, id)
                }
                _ => Err(e),
            },
            ok => ok,
        };
        self.stack.pop();
        match prev {
            Some(v) => doc.ingest.insert("pipeline".into(), v),
            None => doc.ingest.remove("pipeline"),
        };
        result
    }

    fn processors(&mut self, procs: &[Value], doc: &mut Doc) -> R<Flow> {
        for p in procs {
            if let Flow::Drop = self.processor(p, doc)? {
                return Ok(Flow::Drop);
            }
        }
        Ok(Flow::Continue)
    }

    fn on_failure(
        &mut self,
        handlers: &[Value],
        doc: &mut Doc,
        e: &IngestError,
        kind: &str,
        tag: &str,
        pipeline: &str,
    ) -> R<Flow> {
        let saved: Vec<(&str, Option<Value>)> = [
            "on_failure_message",
            "on_failure_processor_type",
            "on_failure_processor_tag",
            "on_failure_pipeline",
        ]
        .iter()
        .map(|k| (*k, doc.ingest.get(*k).cloned()))
        .collect();
        doc.ingest.insert("on_failure_message".into(), json!(e.reason));
        doc.ingest.insert("on_failure_processor_type".into(), json!(kind));
        doc.ingest.insert("on_failure_processor_tag".into(), json!(tag));
        doc.ingest.insert("on_failure_pipeline".into(), json!(pipeline));
        let r = self.processors(handlers, doc);
        for (k, v) in saved {
            match v {
                Some(v) => doc.ingest.insert(k.into(), v),
                None => doc.ingest.remove(k),
            };
        }
        r
    }

    fn processor(&mut self, p: &Value, doc: &mut Doc) -> R<Flow> {
        let (kind, cfg) = processor_parts(p);
        let tag = opt_str(cfg, "tag").unwrap_or("");
        let mut entry = Map::new();
        entry.insert("processor_type".into(), json!(kind));
        if let Some(t) = cfg.get("tag") {
            entry.insert("tag".into(), t.clone());
        }
        if let Some(d) = cfg.get("description") {
            entry.insert("description".into(), d.clone());
        }
        if let Some(cond) = cfg.get("if") {
            let (src, params) = match cond {
                Value::String(s) => (s.clone(), json!({})),
                other => painless::script_parts(other).map_err(IngestError::iae)?,
            };
            let ok = condition(&src, params, doc)?;
            if self.trace.on {
                entry.insert("if".into(), json!({"condition": src, "result": ok}));
            }
            if !ok {
                entry.insert("status".into(), json!("skipped"));
                self.trace.push(Value::Object(entry));
                return Ok(Flow::Continue);
            }
        }
        let result = if kind == "pipeline" {
            let name = render(opt_str(cfg, "name").unwrap_or(""), doc);
            match self.store.get(&name) {
                Err(e) => Err(e),
                Ok(None) if opt_bool(cfg, "ignore_missing_pipeline", false) => Ok(Flow::Continue),
                Ok(None) => Err(IngestError::iae(format!(
                    "Pipeline processor configured for non-existent pipeline [{name}]"
                ))),
                Ok(Some(_)) => {
                    entry.insert("status".into(), json!("success"));
                    entry.insert("doc".into(), sim_doc(doc, true));
                    self.trace.push(Value::Object(entry.clone()));
                    entry.remove("doc");
                    self.pipeline(&name, doc)
                }
            }
        } else if kind == "foreach" {
            self.foreach(cfg, doc)
        } else {
            execute(kind, cfg, doc, self.store)
        };
        match result {
            Ok(Flow::Drop) => {
                entry.insert("status".into(), json!("dropped"));
                self.trace.push(Value::Object(entry));
                Ok(Flow::Drop)
            }
            Ok(Flow::Continue) => {
                if kind != "pipeline" {
                    entry.insert("status".into(), json!("success"));
                    entry.insert("doc".into(), sim_doc(doc, true));
                    self.trace.push(Value::Object(entry));
                }
                Ok(Flow::Continue)
            }
            Err(mut e) => {
                if e.failed.is_none() {
                    e.failed = Some((kind.to_string(), tag.to_string()));
                }
                if opt_bool(cfg, "ignore_failure", false) {
                    entry.insert("status".into(), json!("error_ignored"));
                    entry.insert("ignored_error".into(), json!({"error": e.error_object()}));
                    entry.insert("doc".into(), sim_doc(doc, true));
                    self.trace.push(Value::Object(entry));
                    return Ok(Flow::Continue);
                }
                if let Some(handlers) = cfg.get("on_failure").and_then(Value::as_array) {
                    entry.insert("status".into(), json!("error"));
                    entry.insert("error".into(), e.error_object());
                    self.trace.push(Value::Object(entry));
                    let pipeline = self.stack.last().cloned().unwrap_or_default();
                    let handlers = handlers.clone();
                    return self.on_failure(&handlers, doc, &e, kind, tag, &pipeline);
                }
                entry.insert("status".into(), json!("error"));
                entry.insert("error".into(), e.error_object());
                self.trace.push(Value::Object(entry));
                Err(e)
            }
        }
    }

    fn foreach(&mut self, cfg: &Map<String, Value>, doc: &mut Doc) -> R<Flow> {
        let field = render(opt_str(cfg, "field").unwrap_or(""), doc);
        let Some(v) = doc.get_opt(&field) else {
            if opt_bool(cfg, "ignore_missing", false) {
                return Ok(Flow::Continue);
            }
            return Err(IngestError::iae(format!(
                "field [{field}] not present as part of path [{field}]"
            )));
        };
        let inner = cfg.get("processor").cloned().unwrap_or(Value::Null);
        let saved = (doc.ingest.remove("_value"), doc.ingest.remove("_key"));
        let was_on = std::mem::replace(&mut self.trace.on, false);
        let out = match v {
            Value::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    doc.ingest.insert("_value".into(), item);
                    if let Flow::Drop = self.processor(&inner, doc)? {
                        self.trace.on = was_on;
                        return Ok(Flow::Drop);
                    }
                    out.push(doc.ingest.remove("_value").unwrap_or(Value::Null));
                }
                Value::Array(out)
            }
            Value::Object(m) => {
                let mut out = Map::new();
                for (k, item) in m {
                    doc.ingest.insert("_key".into(), json!(k));
                    doc.ingest.insert("_value".into(), item);
                    if let Flow::Drop = self.processor(&inner, doc)? {
                        self.trace.on = was_on;
                        return Ok(Flow::Drop);
                    }
                    let key = doc.ingest.remove("_key").map(|k| java_string(&k)).unwrap_or(k);
                    out.insert(key, doc.ingest.remove("_value").unwrap_or(Value::Null));
                }
                Value::Object(out)
            }
            Value::Null if opt_bool(cfg, "ignore_missing", false) => {
                self.trace.on = was_on;
                return Ok(Flow::Continue);
            }
            other => {
                self.trace.on = was_on;
                return Err(IngestError::iae(format!(
                    "field [{field}] of type [{}] cannot be cast to a list or map",
                    java_type(&other)
                )));
            }
        };
        self.trace.on = was_on;
        // Elasticsearch leaves `_ingest._value` behind (null).
        doc.ingest.insert("_value".into(), saved.0.unwrap_or(Value::Null));
        if let Some(k) = saved.1 {
            doc.ingest.insert("_key".into(), k);
        }
        doc.set(&field, out)?;
        Ok(Flow::Continue)
    }
}

/// An `if` condition's verdict.
fn condition(src: &str, params: Value, doc: &Doc) -> R<bool> {
    let script = painless::compile(src).map_err(|e| script_error(src, &e, true))?;
    let mut vars = Map::new();
    vars.insert("ctx".into(), Value::Object(doc.ctx.clone()));
    vars.insert("params".into(), params);
    match script.value(vars).map_err(|e| script_error(src, &e, false))? {
        Value::Bool(b) => Ok(b),
        other => Err(IngestError::iae(format!(
            "condition [{src}] returned a non-boolean value of type [{}]",
            java_type(&other)
        ))),
    }
}

fn script_error(src: &str, reason: &str, compile: bool) -> IngestError {
    let mut e = IngestError::new(
        "script_exception",
        if compile { "compile error" } else { "runtime error" },
        400,
    );
    e.extra.insert("script_stack".into(), json!([]));
    e.extra.insert("script".into(), json!(src));
    e.extra.insert("lang".into(), json!("painless"));
    e.caused("illegal_argument_exception", reason)
}

/// The field a string processor reads, as a string (or list of them).
fn string_op(
    kind: &str,
    cfg: &Map<String, Value>,
    doc: &mut Doc,
    f: impl Fn(&str) -> R<Value>,
) -> R<Flow> {
    let field = render(opt_str(cfg, "field").unwrap_or(""), doc);
    let target =
        opt_str(cfg, "target_field").map(|t| render(t, doc)).unwrap_or_else(|| field.clone());
    let ignore_missing = opt_bool(cfg, "ignore_missing", false);
    let v = match doc.get(&field) {
        Ok(v) => v,
        Err(_) if ignore_missing => return Ok(Flow::Continue),
        Err(e) => return Err(e),
    };
    let out = match &v {
        Value::Null if ignore_missing => return Ok(Flow::Continue),
        Value::Null => {
            return Err(IngestError::iae(format!("field [{field}] is null, cannot process it.")));
        }
        Value::String(s) => f(s)?,
        Value::Array(items) if kind != "bytes" => {
            let mut out = Vec::new();
            for x in items {
                match x {
                    Value::String(s) => out.push(f(s)?),
                    other => {
                        return Err(IngestError::iae(format!(
                            "value [{}] of type [{}] in list field [{field}] cannot be cast to [java.lang.String]",
                            java_string(other),
                            java_type(other)
                        )));
                    }
                }
            }
            Value::Array(out)
        }
        other => {
            return Err(IngestError::iae(format!(
                "field [{field}] of type [{}] cannot be cast to [java.lang.String]",
                java_type(other)
            )));
        }
    };
    doc.set(&target, out)?;
    Ok(Flow::Continue)
}

/// Java replacement syntax (`$1`) in regex-lite's (`${1}`).
fn java_replacement(r: &str) -> String {
    let mut out = String::new();
    let mut chars = r.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(n) = chars.next() {
                    if n == '$' {
                        out.push_str("$$");
                    } else {
                        out.push(n);
                    }
                }
            }
            '$' => {
                let mut num = String::new();
                while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
                    num.push(*d);
                    chars.next();
                }
                if num.is_empty() {
                    out.push_str("$$");
                } else {
                    out.push_str(&format!("${{{num}}}"));
                }
            }
            other => out.push(other),
        }
    }
    out
}

fn regex(p: &str) -> R<regex_lite::Regex> {
    regex_lite::Regex::new(p)
        .map_err(|e| IngestError::iae(format!("invalid regular expression [{p}]: {e}")))
}

/// Java's `String.split`: trailing empty strings dropped.
fn java_split(s: &str, re: &regex_lite::Regex, keep_trailing: bool) -> Vec<String> {
    let mut parts: Vec<String> = re.split(s).map(String::from).collect();
    // Java drops a leading empty string only for a zero-width match.
    if parts.len() > 1
        && parts.first().is_some_and(String::is_empty)
        && re.find(s).is_some_and(|m| m.start() == 0 && m.end() == 0)
    {
        parts.remove(0);
    }
    if !keep_trailing {
        while parts.len() > 1 && parts.last().is_some_and(String::is_empty) {
            parts.pop();
        }
    }
    parts
}

fn convert(v: &Value, to: &str) -> R<Value> {
    let fail = |s: &str| IngestError::iae(format!("unable to convert [{s}] to {to}"));
    let text = java_string(v);
    let t = text.trim();
    match to {
        "string" => Ok(json!(text)),
        "integer" | "long" => {
            let parsed = if let Some(h) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
                i64::from_str_radix(h, 16).ok()
            } else if let Some(h) = t.strip_prefix("-0x") {
                i64::from_str_radix(h, 16).ok().map(|x| -x)
            } else {
                t.parse::<i64>().ok()
            };
            let n = match (parsed, v) {
                (Some(n), _) => n,
                (None, Value::Number(n)) if n.is_f64() => n.as_f64().unwrap_or(0.0) as i64,
                _ => {
                    return Err(fail(&text).caused(
                        "number_format_exception",
                        &format!("For input string: \"{text}\""),
                    ));
                }
            };
            if to == "integer" && i32::try_from(n).is_err() {
                return Err(fail(&text)
                    .caused("number_format_exception", &format!("For input string: \"{text}\"")));
            }
            Ok(json!(n))
        }
        "float" | "double" => match t.parse::<f64>() {
            Ok(f) if f.is_finite() => Ok(json!(f)),
            _ => Err(fail(&text)
                .caused("number_format_exception", &format!("For input string: \"{text}\""))),
        },
        "boolean" => match t.to_ascii_lowercase().as_str() {
            "true" => Ok(json!(true)),
            "false" => Ok(json!(false)),
            _ => Err(IngestError::iae(format!(
                "[{text}] is not a boolean value, cannot convert to boolean"
            ))),
        },
        "ip" => {
            if t.parse::<std::net::IpAddr>().is_ok() {
                Ok(json!(t))
            } else {
                Err(IngestError::iae(format!("'{text}' is not an IP string literal.")))
            }
        }
        _ => {
            // auto: the narrowest type the string parses as.
            let Value::String(s) = v else { return Ok(v.clone()) };
            if let Ok(n) = s.parse::<i64>() {
                return Ok(json!(n));
            }
            if let Ok(f) = s.parse::<f64>()
                && f.is_finite()
            {
                return Ok(json!(f));
            }
            match s.as_str() {
                "true" => Ok(json!(true)),
                "false" => Ok(json!(false)),
                _ => Ok(v.clone()),
            }
        }
    }
}

/// One processor (other than `pipeline` and `foreach`) applied to `doc`.
fn execute(kind: &str, cfg: &Map<String, Value>, doc: &mut Doc, store: &Store) -> R<Flow> {
    let field = || opt_str(cfg, "field").unwrap_or("");
    let ignore_missing = opt_bool(cfg, "ignore_missing", false);
    match kind {
        "set" => {
            let field = render(field(), doc);
            let value = match (cfg.get("copy_from").and_then(Value::as_str), cfg.get("value")) {
                (Some(from), _) => doc.get(from)?,
                (None, Some(v)) => render_value(v, doc),
                (None, None) => Value::Null,
            };
            let templated =
                cfg.get("value").and_then(Value::as_str).is_some_and(|v| v.contains("{{"))
                    || cfg.contains_key("copy_from");
            if opt_bool(cfg, "ignore_empty_value", false)
                && templated
                && (value.is_null() || value.as_str() == Some(""))
            {
                return Ok(Flow::Continue);
            }
            if !opt_bool(cfg, "override", true) && doc.get_opt(&field).is_some_and(|x| !x.is_null())
            {
                return Ok(Flow::Continue);
            }
            doc.set(&field, value)?;
        }
        "remove" => {
            if let Some(keep) = cfg.get("keep") {
                let keep: Vec<String> = match keep {
                    Value::Array(a) => a.iter().map(|x| render(&java_string(x), doc)).collect(),
                    other => vec![render(&java_string(other), doc)],
                };
                let mut kept = Map::new();
                for k in &keep {
                    if let Some(v) = doc.get_opt(k) {
                        set_path(&mut kept, k, v, k)?;
                    }
                }
                for m in META {
                    if let Some(v) = doc.ctx.get(*m) {
                        kept.insert(m.to_string(), v.clone());
                    }
                }
                doc.ctx = kept;
                return Ok(Flow::Continue);
            }
            let fields: Vec<String> = match cfg.get("field") {
                Some(Value::Array(a)) => a.iter().map(|x| render(&java_string(x), doc)).collect(),
                Some(other) => vec![render(&java_string(other), doc)],
                None => vec![],
            };
            for f in fields {
                if META.contains(&f.as_str()) && f != "_routing" {
                    return Err(IngestError::iae(format!("cannot remove metadata field [{f}]")));
                }
                match doc.remove(&f) {
                    Ok(_) => {}
                    Err(_) if ignore_missing => {}
                    Err(e) => return Err(e),
                }
            }
        }
        "rename" => {
            let from = render(field(), doc);
            let to = render(opt_str(cfg, "target_field").unwrap_or(""), doc);
            if !doc.has(&from) {
                if ignore_missing {
                    return Ok(Flow::Continue);
                }
                return Err(IngestError::iae(format!("field [{from}] doesn't exist")));
            }
            if doc.has(&to) && !opt_bool(cfg, "override", false) {
                return Err(IngestError::iae(format!("field [{to}] already exists")));
            }
            let v = doc.remove(&from)?;
            if let Err(e) = doc.set(&to, v.clone()) {
                let _ = doc.set(&from, v);
                return Err(e);
            }
        }
        "lowercase" => return string_op(kind, cfg, doc, |s| Ok(json!(s.to_lowercase()))),
        "uppercase" => return string_op(kind, cfg, doc, |s| Ok(json!(s.to_uppercase()))),
        "trim" => return string_op(kind, cfg, doc, |s| Ok(json!(s.trim()))),
        "urldecode" => {
            return string_op(kind, cfg, doc, |s| {
                let mut out = Vec::new();
                let b = s.as_bytes();
                let mut i = 0;
                while i < b.len() {
                    match b[i] {
                        b'+' => out.push(b' '),
                        b'%' => {
                            let hex = s.get(i + 1..i + 3).unwrap_or("");
                            match u8::from_str_radix(hex, 16) {
                                Ok(x) if hex.len() == 2 => {
                                    out.push(x);
                                    i += 2;
                                }
                                _ if hex.len() < 2 => {
                                    return Err(IngestError::iae(
                                        "URLDecoder: Incomplete trailing escape (%) pattern",
                                    ));
                                }
                                _ => {
                                    return Err(IngestError::iae(format!(
                                        "URLDecoder: Illegal hex characters in escape (%) pattern - Error at index 0 in: \"{hex}\""
                                    )));
                                }
                            }
                        }
                        c => out.push(c),
                    }
                    i += 1;
                }
                Ok(json!(String::from_utf8_lossy(&out)))
            });
        }
        "html_strip" => {
            return string_op(kind, cfg, doc, |s| {
                // Block elements become line breaks, inline ones vanish.
                const BLOCK: &[&str] = &[
                    "address",
                    "article",
                    "aside",
                    "blockquote",
                    "br",
                    "center",
                    "dd",
                    "div",
                    "dl",
                    "dt",
                    "fieldset",
                    "figcaption",
                    "figure",
                    "footer",
                    "form",
                    "h1",
                    "h2",
                    "h3",
                    "h4",
                    "h5",
                    "h6",
                    "header",
                    "hr",
                    "li",
                    "main",
                    "nav",
                    "ol",
                    "p",
                    "pre",
                    "section",
                    "table",
                    "td",
                    "th",
                    "tr",
                    "ul",
                ];
                let mut out = String::new();
                let mut tag: Option<String> = None;
                for c in s.chars() {
                    match (&mut tag, c) {
                        (None, '<') => tag = Some(String::new()),
                        (Some(t), '>') => {
                            let name: String = t
                                .trim_start_matches('/')
                                .chars()
                                .take_while(|c| c.is_ascii_alphanumeric())
                                .collect::<String>()
                                .to_ascii_lowercase();
                            if BLOCK.contains(&name.as_str()) {
                                out.push('\n');
                            }
                            tag = None;
                        }
                        (Some(t), c) => t.push(c),
                        (None, c) => out.push(c),
                    }
                }
                let out = out
                    .replace("&amp;", "&")
                    .replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&quot;", "\"")
                    .replace("&nbsp;", "\u{a0}");
                Ok(json!(out))
            });
        }
        "bytes" => {
            return string_op(kind, cfg, doc, |s| {
                super::lifecycle::parse_bytes(s).map(|b| json!(b)).ok_or_else(|| {
                    IngestError::new(
                        "parse_exception",
                        format!("failed to parse setting [Ingest Field] with value [{s}] as a size in bytes: unit is missing or unrecognized"),
                        400,
                    )
                })
            });
        }
        "append" => {
            let f = render(field(), doc);
            let values: Vec<Value> =
                match render_value(cfg.get("value").unwrap_or(&Value::Null), doc) {
                    Value::Array(a) => a,
                    other => vec![other],
                };
            let dups = opt_bool(cfg, "allow_duplicates", true);
            let mut list = match doc.get_opt(&f) {
                None => vec![],
                Some(Value::Array(a)) => a,
                Some(other) => vec![other],
            };
            for v in values {
                if dups || !list.contains(&v) {
                    list.push(v);
                }
            }
            doc.set(&f, Value::Array(list))?;
        }
        "convert" => {
            let f = render(field(), doc);
            let target =
                opt_str(cfg, "target_field").map(|t| render(t, doc)).unwrap_or_else(|| f.clone());
            let to = opt_str(cfg, "type").unwrap_or("auto");
            let v = match doc.get(&f) {
                Ok(Value::Null) | Err(_) if ignore_missing => return Ok(Flow::Continue),
                Ok(Value::Null) => {
                    return Err(IngestError::iae(format!(
                        "Field [{f}] is null, cannot be converted to type [{to}]"
                    )));
                }
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let out = match &v {
                Value::Array(a) => {
                    Value::Array(a.iter().map(|x| convert(x, to)).collect::<R<Vec<_>>>()?)
                }
                other => convert(other, to)?,
            };
            doc.set(&target, out)?;
        }
        "date" => {
            let f = render(field(), doc);
            let v = doc.get(&f)?;
            let text = java_string(&v);
            let tz_name = opt_str(cfg, "timezone").map(|t| render(t, doc));
            let tz = match tz_name.as_deref() {
                None | Some("UTC") | Some("Z") | Some("GMT") => 0,
                Some(t) => super::super::dates::parse_offset(t).ok_or_else(|| {
                    IngestError::iae(format!("The datetime zone id '{t}' is not recognised"))
                })?,
            };
            let formats: Vec<String> = match cfg.get("formats") {
                Some(Value::Array(a)) => a.iter().map(java_string).collect(),
                Some(other) => vec![java_string(other)],
                None => vec![],
            };
            let mut ms = None;
            for fmt in &formats {
                ms = match fmt.as_str() {
                    "ISO8601" => {
                        super::super::dates::parse(&text, Some("strict_date_optional_time"), tz)
                    }
                    "UNIX" => text.parse::<f64>().ok().map(|s| (s * 1000.0) as i64),
                    "UNIX_MS" => text.parse::<i64>().ok(),
                    other => super::super::dates::parse(&text, Some(other), tz),
                };
                if ms.is_some() {
                    break;
                }
            }
            let Some(ms) = ms else {
                let shown: Vec<String> = formats
                    .iter()
                    .map(|f| match f.as_str() {
                        "ISO8601" | "UNIX" | "UNIX_MS" | "TAI64N" => f.to_lowercase(),
                        other => other.to_string(),
                    })
                    .collect();
                let inner = if formats.iter().all(|f| f == "ISO8601") {
                    "Failed to parse with all enclosed parsers".to_string()
                } else {
                    format!("Text '{text}' could not be parsed at index 0")
                };
                let mut e = IngestError::iae(format!("unable to parse date [{text}]"));
                e.caused_by = Some(json!({
                    "type": "illegal_argument_exception",
                    "reason": format!("failed to parse date field [{text}] with format [{}]", shown.join("||")),
                    "caused_by": {"type": "date_time_parse_exception", "reason": inner},
                }));
                return Err(e);
            };
            let target = opt_str(cfg, "target_field")
                .map(|t| render(t, doc))
                .unwrap_or_else(|| "@timestamp".into());
            let out = match opt_str(cfg, "output_format") {
                Some(p) => super::super::dates::format(ms, Some(p), tz),
                None => super::super::dates::format(ms, None, tz),
            };
            doc.set(&target, json!(out))?;
        }
        "split" => {
            let re = regex(opt_str(cfg, "separator").unwrap_or(","))?;
            let keep = opt_bool(cfg, "preserve_trailing", false);
            return string_op(kind, cfg, doc, |s| Ok(json!(java_split(s, &re, keep))));
        }
        "join" => {
            let f = render(field(), doc);
            let sep = opt_str(cfg, "separator").unwrap_or("");
            let target =
                opt_str(cfg, "target_field").map(|t| render(t, doc)).unwrap_or_else(|| f.clone());
            match doc.get(&f)? {
                Value::Array(a) => {
                    let s = a.iter().map(java_string).collect::<Vec<_>>().join(sep);
                    doc.set(&target, json!(s))?;
                }
                other => {
                    return Err(IngestError::iae(format!(
                        "field [{f}] of type [{}] cannot be cast to [java.util.List]",
                        java_type(&other)
                    )));
                }
            }
        }
        "gsub" => {
            let re = regex(opt_str(cfg, "pattern").unwrap_or(""))?;
            let rep = java_replacement(opt_str(cfg, "replacement").unwrap_or(""));
            return string_op(kind, cfg, doc, |s| Ok(json!(re.replace_all(s, rep.as_str()))));
        }
        "grok" => {
            let f = render(field(), doc);
            let v = match doc.get(&f) {
                Ok(Value::Null) | Err(_) if ignore_missing => return Ok(Flow::Continue),
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let Value::String(s) = v else {
                return Err(IngestError::iae(format!(
                    "field [{f}] of type [{}] cannot be cast to [java.lang.String]",
                    java_type(&v)
                )));
            };
            let grok = Grok::new(cfg)?;
            let Some((k, captures)) = grok.matches(&s) else {
                return Err(IngestError::iae(format!(
                    "Provided Grok expressions do not match field value: [{s}]"
                )));
            };
            for (name, v) in captures {
                doc.set(&name, v)?;
            }
            if opt_bool(cfg, "trace_match", false) {
                doc.ingest.insert("_grok_match_index".into(), json!(k.to_string()));
            }
        }
        "dissect" => {
            let f = render(field(), doc);
            let v = match doc.get(&f) {
                Ok(Value::Null) | Err(_) if ignore_missing => return Ok(Flow::Continue),
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let s = java_string(&v);
            let pattern = opt_str(cfg, "pattern").unwrap_or("");
            let sep = opt_str(cfg, "append_separator").unwrap_or("");
            let out = dissect(pattern, &s, sep).ok_or_else(|| {
                IngestError::new(
                    "find_match",
                    format!(
                        "Unable to find match for dissect pattern: {pattern} against source: {s}"
                    ),
                    400,
                )
            })?;
            for (k, v) in out {
                doc.set(&k, json!(v))?;
            }
        }
        "script" => {
            let src = match (
                cfg.get("id").and_then(Value::as_str),
                opt_str(cfg, "source").or_else(|| opt_str(cfg, "inline")),
            ) {
                (Some(id), _) => store
                    .scripts
                    .get(id)
                    .and_then(|s| s.get("source"))
                    .and_then(Value::as_str)
                    .map(String::from)
                    .ok_or_else(|| {
                        IngestError::new(
                            "resource_not_found_exception",
                            format!("unable to find script [{id}] in cluster state"),
                            404,
                        )
                    })?,
                (None, Some(s)) => s.to_string(),
                (None, None) => String::new(),
            };
            let params = cfg.get("params").cloned().unwrap_or_else(|| json!({}));
            let script = painless::compile(&src).map_err(|e| script_error(&src, &e, true))?;
            let ctx = Value::Object(doc.ctx.clone());
            let out = script.run(ctx, params).map_err(|e| script_error(&src, &e, false))?;
            match out {
                Value::Object(m) => doc.ctx = m,
                _ => return Err(IngestError::iae("ctx must be a map")),
            }
        }
        "fail" => {
            let msg = render(opt_str(cfg, "message").unwrap_or(""), doc);
            return Err(IngestError::new("fail_processor_exception", msg, 500));
        }
        "drop" => return Ok(Flow::Drop),
        "json" => {
            let f = render(field(), doc);
            let v = doc.get(&f)?;
            let parsed: Value = match &v {
                Value::String(s) => serde_json::from_str(s).map_err(|e| {
                    IngestError::iae(format!("com.fasterxml.jackson.core.JsonParseException: {e}"))
                })?,
                other => other.clone(),
            };
            if opt_bool(cfg, "add_to_root", false) {
                let Value::Object(m) = parsed else {
                    return Err(IngestError::iae("cannot add non-map fields to root of document"));
                };
                let merge = opt_str(cfg, "add_to_root_conflict_strategy") == Some("merge");
                for (k, x) in m {
                    if merge
                        && let (Some(Value::Object(old)), Value::Object(new)) =
                            (doc.ctx.get_mut(&k), &x)
                    {
                        for (a, b) in new {
                            old.insert(a.clone(), b.clone());
                        }
                        continue;
                    }
                    doc.ctx.insert(k, x);
                }
            } else {
                let target = opt_str(cfg, "target_field")
                    .map(|t| render(t, doc))
                    .unwrap_or_else(|| f.clone());
                doc.set(&target, parsed)?;
            }
        }
        "kv" => {
            let f = render(field(), doc);
            let v = match doc.get(&f) {
                Ok(Value::Null) | Err(_) if ignore_missing => return Ok(Flow::Continue),
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let s = java_string(&v);
            let fs = regex(opt_str(cfg, "field_split").unwrap_or(" "))?;
            let vs = regex(opt_str(cfg, "value_split").unwrap_or("="))?;
            let list = |k: &str| -> Option<Vec<String>> {
                cfg.get(k).and_then(Value::as_array).map(|a| a.iter().map(java_string).collect())
            };
            let (include, exclude) = (list("include_keys"), list("exclude_keys"));
            let prefix = opt_str(cfg, "prefix").unwrap_or("");
            let trim = |s: &str, k: &str| -> String {
                match opt_str(cfg, k) {
                    Some(chars) => s.trim_matches(|c| chars.contains(c)).to_string(),
                    None => s.to_string(),
                }
            };
            let strip = opt_bool(cfg, "strip_brackets", false);
            let target = opt_str(cfg, "target_field").map(|t| render(t, doc));
            let mut out: Vec<(String, Value)> = Vec::new();
            for pair in java_split(&s, &fs, false) {
                let mut kv = vs.splitn(&pair, 2);
                let k = trim(kv.next().unwrap_or(""), "trim_key");
                let Some(val) = kv.next() else {
                    return Err(IngestError::iae(format!(
                        "field [{f}] does not contain value_split [{}]",
                        opt_str(cfg, "value_split").unwrap_or("=")
                    )));
                };
                let mut val = trim(val, "trim_value");
                if strip {
                    val = val
                        .trim_matches(|c| {
                            matches!(c, '(' | ')' | '<' | '>' | '[' | ']' | '"' | '\'')
                        })
                        .to_string();
                }
                if include.as_ref().is_some_and(|i| !i.contains(&k))
                    || exclude.as_ref().is_some_and(|e| e.contains(&k))
                {
                    continue;
                }
                let key = format!("{prefix}{k}");
                match out.iter_mut().find(|(x, _)| *x == key) {
                    Some((_, Value::Array(a))) => a.push(json!(val)),
                    Some((_, old)) => *old = json!([old.clone(), val]),
                    None => out.push((key, json!(val))),
                }
            }
            for (k, v) in out {
                let path = match &target {
                    Some(t) => format!("{t}.{k}"),
                    None => k,
                };
                doc.set(&path, v)?;
            }
        }
        "dot_expander" => {
            let f = render(field(), doc);
            let path = opt_str(cfg, "path").map(String::from);
            let over = opt_bool(cfg, "override", false);
            let map: &mut Map<String, Value> = match &path {
                Some(p) => {
                    let mut cur = &mut doc.ctx;
                    for part in p.split('.') {
                        match cur.get_mut(part) {
                            Some(Value::Object(m)) => cur = m,
                            _ => {
                                return Err(IngestError::iae(format!(
                                    "field [{part}] not present as part of path [{p}]"
                                )));
                            }
                        }
                    }
                    cur
                }
                None => &mut doc.ctx,
            };
            let keys: Vec<String> = if f == "*" {
                map.keys().filter(|k| k.contains('.')).cloned().collect()
            } else {
                vec![f.clone()]
            };
            for k in keys {
                let Some(v) = map.remove(&k) else { continue };
                if !k.contains('.') {
                    map.insert(k, v);
                    continue;
                }
                let existing = get_path(map, &k, &k).ok().cloned();
                let v = match (existing, over) {
                    (Some(old), false) => match old {
                        Value::Array(mut a) => {
                            a.push(v);
                            Value::Array(a)
                        }
                        other => json!([other, v]),
                    },
                    _ => v,
                };
                set_path(map, &k, v, &k)?;
            }
        }
        "sort" => {
            let f = render(field(), doc);
            let target =
                opt_str(cfg, "target_field").map(|t| render(t, doc)).unwrap_or_else(|| f.clone());
            let Value::Array(mut a) = doc.get(&f)? else {
                return Err(IngestError::iae(format!(
                    "field [{f}] of type [java.lang.String] cannot be cast to [java.util.List]"
                )));
            };
            a.sort_by(|x, y| match (x.as_f64(), y.as_f64()) {
                (Some(p), Some(q)) => p.partial_cmp(&q).unwrap_or(std::cmp::Ordering::Equal),
                _ => java_string(x).cmp(&java_string(y)),
            });
            if opt_str(cfg, "order") == Some("desc") {
                a.reverse();
            }
            doc.set(&target, Value::Array(a))?;
        }
        "reroute" => {
            if let Some(d) = opt_str(cfg, "destination") {
                let d = render(d, doc);
                doc.ctx.insert("_index".into(), json!(d));
            }
        }
        "csv" => {
            let f = render(field(), doc);
            let v = match doc.get(&f) {
                Ok(Value::Null) | Err(_) if ignore_missing => return Ok(Flow::Continue),
                Ok(v) => v,
                Err(e) => return Err(e),
            };
            let s = java_string(&v);
            let sep = opt_str(cfg, "separator").and_then(|x| x.chars().next()).unwrap_or(',');
            let quote = opt_str(cfg, "quote").and_then(|x| x.chars().next()).unwrap_or('"');
            let trim = opt_bool(cfg, "trim", false);
            let mut cells = vec![String::new()];
            let mut quoted = false;
            let mut chars = s.chars().peekable();
            while let Some(c) = chars.next() {
                if c == quote {
                    if quoted && chars.peek() == Some(&quote) {
                        cells.last_mut().unwrap().push(quote);
                        chars.next();
                    } else {
                        quoted = !quoted;
                    }
                } else if c == sep && !quoted {
                    cells.push(String::new());
                } else {
                    cells.last_mut().unwrap().push(c);
                }
            }
            let targets: Vec<String> = match cfg.get("target_fields") {
                Some(Value::Array(a)) => a.iter().map(java_string).collect(),
                Some(other) => vec![java_string(other)],
                None => vec![],
            };
            for (t, cell) in targets.iter().zip(cells) {
                let cell = if trim { cell.trim().to_string() } else { cell };
                if cell.is_empty() {
                    if let Some(ev) = cfg.get("empty_value") {
                        doc.set(t, ev.clone())?;
                    }
                    continue;
                }
                doc.set(t, json!(cell))?;
            }
        }
        other => {
            return Err(IngestError::parse(
                format!("No processor type exists with name [{other}]"),
                Some(other),
                None,
            ));
        }
    }
    Ok(Flow::Continue)
}

/// `dissect`: `%{a} %{b}` patterns (with `+` append, `?` and empty
/// skip keys, and `->` right padding).
fn dissect(pattern: &str, s: &str, append_sep: &str) -> Option<Vec<(String, String)>> {
    // Split the pattern into (key, delimiter-after) pieces.
    let mut keys: Vec<(String, String)> = Vec::new();
    let mut rest = pattern;
    let lead = rest.find("%{")?;
    let prefix = &rest[..lead];
    rest = &rest[lead..];
    while let Some(start) = rest.find("%{") {
        let end = rest[start..].find('}')? + start;
        let key = rest[start + 2..end].to_string();
        let after = &rest[end + 1..];
        let next = after.find("%{").unwrap_or(after.len());
        keys.push((key, after[..next].to_string()));
        rest = &after[next..];
    }
    let mut text = s.strip_prefix(prefix)?;
    let mut out: Vec<(String, String)> = Vec::new();
    let n = keys.len();
    for (k, (key, delim)) in keys.iter().enumerate() {
        let (pad, key) = match key.strip_suffix("->") {
            Some(k) => (true, k.to_string()),
            None => (false, key.clone()),
        };
        let value;
        if k + 1 == n && delim.is_empty() {
            value = text.to_string();
            text = "";
        } else {
            let pos = text.find(delim.as_str())?;
            value = text[..pos].to_string();
            text = &text[pos + delim.len()..];
            if pad {
                while let Some(t) = text.strip_prefix(delim.as_str()) {
                    text = t;
                }
            }
        }
        if key.is_empty() || key.starts_with('?') {
            continue;
        }
        if let Some(name) = key.strip_prefix('+') {
            let name = name.split('/').next().unwrap_or(name).to_string();
            match out.iter_mut().find(|(x, _)| *x == name) {
                Some((_, v)) => {
                    v.push_str(append_sep);
                    v.push_str(&value);
                }
                None => out.push((name, value)),
            }
            continue;
        }
        out.push((key, value));
    }
    Some(out)
}

/// The built-in grok patterns noida knows (the common legacy ones).
const GROK_PATTERNS: &[(&str, &str)] = &[
    ("USERNAME", r"[a-zA-Z0-9._-]+"),
    ("USER", r"%{USERNAME}"),
    ("EMAILLOCALPART", r"[a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~]+(?:\.[a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~]+)*"),
    ("EMAILADDRESS", r"%{EMAILLOCALPART}@%{HOSTNAME}"),
    ("INT", r"[+-]?[0-9]+"),
    ("BASE10NUM", r"[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)"),
    ("NUMBER", r"%{BASE10NUM}"),
    ("BASE16NUM", r"[+-]?(?:0x)?[0-9A-Fa-f]+"),
    ("POSINT", r"[1-9][0-9]*"),
    ("NONNEGINT", r"[0-9]+"),
    ("WORD", r"\b\w+\b"),
    ("NOTSPACE", r"\S+"),
    ("SPACE", r"\s*"),
    ("DATA", r".*?"),
    ("GREEDYDATA", r".*"),
    ("QUOTEDSTRING", r#""(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|`(?:[^`\\]|\\.)*`"#),
    ("QS", r"%{QUOTEDSTRING}"),
    ("UUID", r"[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}"),
    ("MAC", r"(?:[A-Fa-f0-9]{2}[:-]){5}[A-Fa-f0-9]{2}|(?:[A-Fa-f0-9]{4}\.){2}[A-Fa-f0-9]{4}"),
    (
        "IPV4",
        r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9]?[0-9])",
    ),
    ("IPV6", r"(?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{0,4}(?:%[0-9A-Za-z]+)?"),
    ("IP", r"%{IPV6}|%{IPV4}"),
    ("HOSTNAME", r"\b[0-9A-Za-z][0-9A-Za-z-]{0,62}(?:\.[0-9A-Za-z][0-9A-Za-z-]{0,62})*\.?\b"),
    ("HOST", r"%{HOSTNAME}"),
    ("IPORHOST", r"%{IP}|%{HOSTNAME}"),
    ("HOSTPORT", r"%{IPORHOST}:%{POSINT}"),
    ("UNIXPATH", r"(?:/[A-Za-z0-9$.+!*'(){},~:;=@#%&_\-]*)+"),
    ("WINPATH", r"(?:[A-Za-z]+:|\\)(?:\\[^\\?*]*)+"),
    ("PATH", r"%{UNIXPATH}|%{WINPATH}"),
    ("URIPROTO", r"[A-Za-z][A-Za-z0-9+\-.]*"),
    ("URIHOST", r"%{IPORHOST}(?::%{POSINT})?"),
    ("URIPATH", r"(?:/[A-Za-z0-9$.+!*'(){},~:;=@#%&_\-]*)+"),
    ("URIPARAM", r"\?[A-Za-z0-9$.+!*'|(){},~@#%&/=:;_?\-\[\]<>]*"),
    ("URIPATHPARAM", r"%{URIPATH}(?:%{URIPARAM})?"),
    ("URI", r"%{URIPROTO}://(?:%{USER}(?::[^@]*)?@)?(?:%{URIHOST})?(?:%{URIPATHPARAM})?"),
    (
        "MONTH",
        r"\b(?:[Jj]an(?:uary|uar)?|[Ff]eb(?:ruary|ruar)?|[Mm](?:a|ä)?r(?:ch|z)?|[Aa]pr(?:il)?|[Mm]a(?:y|i)?|[Jj]un(?:e|i)?|[Jj]ul(?:y|i)?|[Aa]ug(?:ust)?|[Ss]ep(?:tember)?|[Oo](?:c|k)?t(?:ober)?|[Nn]ov(?:ember)?|[Dd]e(?:c|z)(?:ember)?)\b",
    ),
    ("MONTHNUM", r"0?[1-9]|1[0-2]"),
    ("MONTHNUM2", r"0[1-9]|1[0-2]"),
    ("MONTHDAY", r"(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9]"),
    (
        "DAY",
        r"(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)",
    ),
    ("YEAR", r"(?:\d\d){1,2}"),
    ("HOUR", r"2[0123]|[01]?[0-9]"),
    ("MINUTE", r"[0-5][0-9]"),
    ("SECOND", r"(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?"),
    ("TIME", r"%{HOUR}:%{MINUTE}(?::%{SECOND})?"),
    ("DATE_US", r"%{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}"),
    ("DATE_EU", r"%{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}"),
    ("ISO8601_TIMEZONE", r"Z|[+-]%{HOUR}(?::?%{MINUTE})"),
    ("ISO8601_SECOND", r"%{SECOND}"),
    (
        "TIMESTAMP_ISO8601",
        r"%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?",
    ),
    ("DATE", r"%{DATE_US}|%{DATE_EU}"),
    ("DATESTAMP", r"%{DATE}[- ]%{TIME}"),
    ("TZ", r"[A-Z]{3}"),
    ("HTTPDATE", r"%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}"),
    ("SYSLOGTIMESTAMP", r"%{MONTH} +%{MONTHDAY} %{TIME}"),
    (
        "LOGLEVEL",
        r"[Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|[Nn]otice|NOTICE|[Ii]nfo?(?:rmation)?|INFO?(?:RMATION)?|[Ww]arn?(?:ing)?|WARN?(?:ING)?|[Ee]rr?(?:or)?|ERR?(?:OR)?|[Cc]rit?(?:ical)?|CRIT?(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?",
    ),
];

/// A named capture: (regex group, field, conversion type).
type Capture = (String, String, Option<String>);

struct Grok {
    patterns: Vec<(regex_lite::Regex, Vec<Capture>)>,
}

impl Grok {
    fn new(cfg: &Map<String, Value>) -> R<Self> {
        let custom: HashMap<String, String> = cfg
            .get("pattern_definitions")
            .and_then(Value::as_object)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), java_string(v))).collect())
            .unwrap_or_default();
        let list: Vec<String> = match cfg.get("patterns") {
            Some(Value::Array(a)) => a.iter().map(java_string).collect(),
            _ => vec![],
        };
        if list.is_empty() {
            return Err(IngestError::parse(
                "List of patterns must not be empty",
                Some("grok"),
                Some("patterns"),
            ));
        }
        let mut patterns = Vec::new();
        for p in &list {
            let mut names = Vec::new();
            let re = expand_grok(p, &custom, &mut names, 0)?;
            let re = regex_lite::Regex::new(&re).map_err(|e| {
                IngestError::iae(format!("Unable to compile grok pattern [{p}]: {e}"))
            })?;
            patterns.push((re, names));
        }
        Ok(Self { patterns })
    }

    /// The first matching pattern's index and captures.
    fn matches(&self, s: &str) -> Option<(usize, Vec<(String, Value)>)> {
        for (k, (re, names)) in self.patterns.iter().enumerate() {
            let Some(c) = re.captures(s) else { continue };
            let mut out: Vec<(String, Value)> = Vec::new();
            for (group, field, ty) in names {
                let Some(m) = c.name(group) else { continue };
                let text = m.as_str();
                let v = match ty.as_deref() {
                    Some("int") | Some("long") => {
                        text.parse::<i64>().map(|n| json!(n)).unwrap_or(json!(text))
                    }
                    Some("float") | Some("double") => {
                        text.parse::<f64>().map(|n| json!(n)).unwrap_or(json!(text))
                    }
                    Some("boolean") => json!(text.eq_ignore_ascii_case("true")),
                    _ => json!(text),
                };
                match out.iter_mut().find(|(f, _)| f == field) {
                    Some((_, Value::Array(a))) => a.push(v),
                    Some((_, old)) => *old = json!([old.clone(), v]),
                    None => out.push((field.clone(), v)),
                }
            }
            return Some((k, out));
        }
        None
    }
}

/// A grok expression as a regex; named captures become `g{n}` groups.
fn expand_grok(
    p: &str,
    custom: &HashMap<String, String>,
    names: &mut Vec<(String, String, Option<String>)>,
    depth: usize,
) -> R<String> {
    if depth > 30 {
        return Err(IngestError::iae("circular reference in grok pattern"));
    }
    let mut out = String::new();
    let mut rest = p;
    while let Some(start) = rest.find("%{") {
        out.push_str(&rest[..start]);
        let Some(end) = rest[start..].find('}') else {
            out.push_str(&rest[start..]);
            rest = "";
            break;
        };
        let inner = &rest[start + 2..start + end];
        let mut parts = inner.splitn(3, ':');
        let syntax = parts.next().unwrap_or("");
        let semantic = parts.next();
        let ty = parts.next().map(String::from);
        let def = custom
            .get(syntax)
            .cloned()
            .or_else(|| {
                GROK_PATTERNS.iter().find(|(n, _)| *n == syntax).map(|(_, d)| d.to_string())
            })
            .ok_or_else(|| {
                IngestError::iae(format!(
                    "Unable to find pattern [{syntax}] in Grok's pattern dictionary"
                ))
            })?;
        let expanded = expand_grok(&def, custom, names, depth + 1)?;
        match semantic {
            Some(field) => {
                let group = format!("g{}", names.len());
                names.push((
                    group.clone(),
                    field.replace(['[', ']'], ".").trim_matches('.').replace("..", "."),
                    ty,
                ));
                out.push_str(&format!("(?P<{group}>{expanded})"));
            }
            None => out.push_str(&format!("(?:{expanded})")),
        }
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    // Oniguruma-only syntax regex-lite lacks: atomic groups and named
    // groups written `(?<name>`.
    Ok(out.replace("(?>", "(?:"))
}

// ---------------------------------------------------------------------
// The APIs
// ---------------------------------------------------------------------

/// A simulated document as `_ingest/pipeline/_simulate` prints it.
fn sim_doc(doc: &Doc, with_pipeline: bool) -> Value {
    let mut m = Map::new();
    m.insert("_index".into(), doc.ctx.get("_index").cloned().unwrap_or(json!("_index")));
    m.insert("_version".into(), json!("-3"));
    m.insert("_id".into(), doc.ctx.get("_id").cloned().unwrap_or(json!("_id")));
    if let Some(r) = doc.ctx.get("_routing").filter(|r| !r.is_null()) {
        m.insert("_routing".into(), r.clone());
    }
    m.insert("_source".into(), Value::Object(doc.source()));
    let mut ingest = Map::new();
    if with_pipeline && let Some(p) = doc.ingest.get("pipeline") {
        ingest.insert("pipeline".into(), p.clone());
    }
    for (k, v) in &doc.ingest {
        if !matches!(
            k.as_str(),
            "pipeline"
                | "on_failure_message"
                | "on_failure_processor_type"
                | "on_failure_processor_tag"
                | "on_failure_pipeline"
        ) {
            ingest.insert(k.clone(), v.clone());
        }
    }
    m.insert("_ingest".into(), Value::Object(ingest));
    Value::Object(m)
}

/// The final document of a write, or why there is none.
pub(super) enum Ingested {
    /// Index this (`_index`, `_id`, `_routing`, source) with the pipelines
    /// that ran.
    Doc {
        index: String,
        id: Option<String>,
        routing: Option<String>,
        source: Value,
        executed: Vec<String>,
    },
    Dropped,
    Failed(IngestError),
}

/// An index's `index.default_pipeline` / `index.final_pipeline`, from its
/// settings or (before it exists) its matching templates.
fn index_pipelines(s: &State, index: &str) -> (Option<String>, Option<String>) {
    let settings = match s.indices.get(index) {
        Some(i) => i.settings.clone(),
        None => s.templates.resolve(index).map(|r| r.settings).unwrap_or_else(|| json!({})),
    };
    let flat = super::flat_settings(&settings);
    let get = |k: &str| {
        flat.iter()
            .find(|(f, _)| f == &format!("index.{k}") || f == k)
            .and_then(|(_, v)| v.as_str().map(String::from))
            .filter(|p| p != "_none" && !p.is_empty())
    };
    (get("default_pipeline"), get("final_pipeline"))
}

/// Runs a write's pipelines: the requested (or default) one, then the
/// final one; a pipeline sending the document to another index hands it
/// to that index's pipelines.
fn run_write_pipelines(
    s: &State,
    store: &Store,
    index: &str,
    id: Option<&str>,
    routing: Option<&str>,
    source: Map<String, Value>,
    requested: Option<&str>,
) -> Ingested {
    let mut doc = Doc::new(index, id, routing, source);
    let mut executed = Vec::new();
    let mut runner = Runner { store, stack: vec![], trace: Trace { results: vec![], on: false } };
    let mut target = index.to_string();
    let mut history = vec![target.clone()];
    let (default, mut final_) = index_pipelines(s, &target);
    let mut first = match requested {
        Some("_none") => None,
        Some(p) => Some(p.to_string()),
        None => default,
    };
    loop {
        if let Some(p) = first.take() {
            executed.push(p.clone());
            match runner.pipeline(&p, &mut doc) {
                Err(e) => return Ingested::Failed(e),
                Ok(Flow::Drop) => return Ingested::Dropped,
                Ok(Flow::Continue) => {}
            }
            let now = doc.meta("_index").unwrap_or_default();
            if now != target {
                if history.contains(&now) {
                    history.push(now.clone());
                    return Ingested::Failed(IngestError::new(
                        "illegal_state_exception",
                        format!(
                            "index cycle detected while processing pipeline [{p}] for document [{}]: {}",
                            id.unwrap_or(""),
                            history.join(" -> ")
                        ),
                        500,
                    ));
                }
                history.push(now.clone());
                target = now;
                let (d, f) = index_pipelines(s, &target);
                first = d;
                final_ = f;
                continue;
            }
        }
        break;
    }
    if let Some(p) = final_ {
        executed.push(p.clone());
        match runner.pipeline(&p, &mut doc) {
            Err(e) => return Ingested::Failed(e),
            Ok(Flow::Drop) => return Ingested::Dropped,
            Ok(Flow::Continue) => {}
        }
        if doc.meta("_index").unwrap_or_default() != target {
            return Ingested::Failed(IngestError::new(
                "illegal_state_exception",
                format!("final pipeline [{p}] can't change the target index"),
                500,
            ));
        }
    }
    Ingested::Doc {
        index: doc.meta("_index").unwrap_or_default(),
        id: doc.meta("_id"),
        routing: doc.meta("_routing"),
        source: Value::Object(doc.source()),
        executed,
    }
}

impl Engine {
    /// A document write (`PUT/POST _doc`, `_create`, a bulk item) through
    /// its ingest pipelines, then into its index.
    pub(super) fn index_with_pipelines(
        &self,
        method: &str,
        index: &str,
        id: &str,
        kind: &str,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        if !matches!(method, "PUT" | "POST") || kind == "_source" {
            return self.document_api(method, index, id, kind, q, body);
        }
        let outcome = {
            let s = self.0.lock().unwrap();
            let (default, final_) = index_pipelines(&s, index);
            let requested = q.get("pipeline").map(String::as_str);
            let none = match requested {
                None => default.is_none() && final_.is_none(),
                Some("_none") => final_.is_none(),
                Some(_) => false,
            };
            if none {
                None
            } else {
                let Some(Value::Object(source)) = parse_json(body) else {
                    drop(s);
                    return self.document_api(method, index, id, kind, q, body);
                };
                let store = Store {
                    pipelines: &s.admin.pipelines,
                    substitutions: None,
                    scripts: &s.admin.scripts,
                };
                let id = Some(id).filter(|i| !i.is_empty());
                Some(run_write_pipelines(
                    &s,
                    &store,
                    index,
                    id,
                    q.get("routing").map(String::as_str),
                    source,
                    requested,
                ))
            }
        };
        match outcome {
            None => self.document_api(method, index, id, kind, q, body),
            Some(Ingested::Failed(e)) => e.response(),
            Some(Ingested::Dropped) => (
                200,
                json!({"_index": index, "_id": id, "_version": -3, "result": "noop",
                       "_shards": {"total": 0, "successful": 0, "failed": 0}}),
            ),
            Some(Ingested::Doc { index: new_index, id: new_id, routing, source, .. }) => {
                let mut q = q.clone();
                q.remove("pipeline");
                match routing {
                    Some(r) => q.insert("routing".into(), r),
                    None => q.remove("routing"),
                };
                let target = if new_index == index {
                    new_index
                } else {
                    match self.write_target(&new_index) {
                        Ok(t) => t,
                        Err(e) => return e,
                    }
                };
                let id = new_id.unwrap_or_default();
                self.document_api(method, &target, &id, kind, &q, source.to_string().as_bytes())
            }
        }
    }

    /// Whether a write to `index` with these parameters runs a pipeline.
    pub(super) fn uses_pipelines(&self, index: &str, q: &HashMap<String, String>) -> bool {
        let s = self.0.lock().unwrap();
        let (default, final_) = index_pipelines(&s, index);
        match q.get("pipeline").map(String::as_str) {
            Some("_none") => final_.is_some(),
            Some(_) => true,
            None => default.is_some() || final_.is_some(),
        }
    }

    /// Everything under `/_ingest`.
    pub(super) fn ingest_api(
        &self,
        method: &str,
        seg: &[&str],
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        match (method, &seg[1..]) {
            ("GET", ["pipeline"]) => self.get_pipelines("*", q, true),
            ("GET" | "POST", ["pipeline", "_simulate"]) => self.simulate_pipeline(None, q, body),
            ("GET", ["pipeline", id]) => self.get_pipelines(id, q, false),
            ("PUT", ["pipeline", id]) => self.put_pipeline(id, q, body),
            ("DELETE", ["pipeline", id]) => self.delete_pipeline(id),
            ("GET" | "POST", ["pipeline", id, "_simulate"]) => {
                self.simulate_pipeline(Some(id), q, body)
            }
            ("GET" | "POST", ["_simulate"]) => self.simulate_ingest(None, q, body),
            ("GET" | "POST", [index, "_simulate"]) => self.simulate_ingest(Some(index), q, body),
            ("GET", ["processor", "grok"]) => {
                let patterns: Map<String, Value> =
                    GROK_PATTERNS.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
                (200, json!({"patterns": patterns}))
            }
            _ => super::no_handler(method, &format!("/{}", seg.join("/"))),
        }
    }

    fn get_pipelines(&self, expr: &str, q: &HashMap<String, String>, all: bool) -> (u16, Value) {
        let s = self.0.lock().unwrap();
        let summary = truthy(q, "summary");
        let mut out = Map::new();
        for part in expr.split(',').map(str::trim) {
            for (id, p) in &s.admin.pipelines {
                if part == "*" || part == "_all" || glob_match(part, id) {
                    out.insert(id.clone(), if summary { json!({}) } else { p.clone() });
                }
            }
        }
        // (Elasticsearch always has built-in pipelines, so listing them
        // all never comes back empty there.)
        if out.is_empty() && !all {
            return (404, json!({}));
        }
        (200, Value::Object(out))
    }

    fn put_pipeline(&self, id: &str, q: &HashMap<String, String>, body: &[u8]) -> (u16, Value) {
        let Some(p) = parse_json(body) else { return (400, super::malformed_body()) };
        if let Err(e) = validate_pipeline(id, &p) {
            return e.response();
        }
        let mut s = self.0.lock().unwrap();
        if let Some(want) = q.get("if_version") {
            let have = s.admin.pipelines.get(id).map(|x| x.get("version").and_then(Value::as_i64));
            match have {
                None => {
                    return (
                        404,
                        error(
                            "resource_not_found_exception",
                            &format!("pipeline [{id}] does not exist"),
                            404,
                        ),
                    );
                }
                Some(v) if v.map(|v| v.to_string()).as_deref() != Some(want.as_str()) => {
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!(
                                "version conflict, required version [{want}] for pipeline [{id}] but current version is [{}]",
                                v.map_or("none".into(), |v| v.to_string())
                            ),
                            400,
                        ),
                    );
                }
                _ => {}
            }
        }
        s.admin.pipelines.insert(id.to_string(), p);
        (200, json!({"acknowledged": true}))
    }

    fn delete_pipeline(&self, expr: &str) -> (u16, Value) {
        let mut s = self.0.lock().unwrap();
        let ids: Vec<String> = s
            .admin
            .pipelines
            .keys()
            .filter(|id| expr.split(',').any(|p| p.trim() == "*" || glob_match(p.trim(), id)))
            .cloned()
            .collect();
        if ids.is_empty() {
            return (
                404,
                error(
                    "resource_not_found_exception",
                    &format!("pipeline [{expr}] is missing"),
                    404,
                ),
            );
        }
        // A pipeline an index uses as default or final can't go.
        for id in &ids {
            for n in s.indices.keys() {
                let (d, f) = index_pipelines(&s, n);
                if d.as_deref() == Some(id) || f.as_deref() == Some(id) {
                    let which = if d.as_deref() == Some(id) { "default" } else { "final" };
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!(
                                "pipeline [{id}] cannot be deleted because it is the {which} pipeline for 1 index(es) including [{n}]"
                            ),
                            400,
                        ),
                    );
                }
            }
        }
        for id in ids {
            s.admin.pipelines.remove(&id);
        }
        (200, json!({"acknowledged": true}))
    }

    /// `POST _ingest/pipeline[/{id}]/_simulate`.
    fn simulate_pipeline(
        &self,
        id: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let Some(req) = parse_json(body) else { return (400, super::malformed_body()) };
        let s = self.0.lock().unwrap();
        let (pipeline_id, inline) = match id {
            Some(id) => {
                if !s.admin.pipelines.contains_key(id) {
                    return (
                        400,
                        error(
                            "illegal_argument_exception",
                            &format!("pipeline [{id}] does not exist"),
                            400,
                        ),
                    );
                }
                (id.to_string(), None)
            }
            None => {
                let Some(p) = req.get("pipeline") else {
                    return IngestError::parse(
                        "[pipeline] required property is missing",
                        None,
                        Some("pipeline"),
                    )
                    .response();
                };
                if let Err(e) = validate_pipeline("_simulate_pipeline", p) {
                    return e.response();
                }
                ("_simulate_pipeline".to_string(), Some(p.clone()))
            }
        };
        let Some(docs) = req.get("docs").and_then(Value::as_array) else {
            return IngestError::parse("[docs] required property is missing", None, Some("docs"))
                .response();
        };
        let mut subs = Map::new();
        if let Some(p) = inline {
            subs.insert(pipeline_id.clone(), p);
        }
        let store = Store {
            pipelines: &s.admin.pipelines,
            substitutions: Some(&subs),
            scripts: &s.admin.scripts,
        };
        let verbose = truthy(q, "verbose");
        let mut out = Vec::new();
        for d in docs {
            let Some(src) = d.get("_source").and_then(Value::as_object) else {
                return IngestError::parse(
                    "[_source] required property is missing",
                    None,
                    Some("_source"),
                )
                .response();
            };
            let meta = |k: &str| d.get(k).map(java_string);
            let mut doc = Doc::new(
                &meta("_index").unwrap_or_else(|| "_index".into()),
                Some(&meta("_id").unwrap_or_else(|| "_id".into())),
                meta("_routing").as_deref(),
                src.clone(),
            );
            let mut runner = Runner {
                store: &store,
                stack: vec![],
                trace: Trace { results: vec![], on: verbose },
            };
            let r = runner.pipeline(&pipeline_id, &mut doc);
            if verbose {
                out.push(json!({"processor_results": runner.trace.results}));
                continue;
            }
            out.push(match r {
                Ok(Flow::Continue) => json!({"doc": sim_doc(&doc, false)}),
                Ok(Flow::Drop) => Value::Null,
                Err(e) => json!({"error": e.error_object()}),
            });
        }
        (200, json!({"docs": out}))
    }

    /// `POST _ingest[/{index}]/_simulate`: the pipelines each document
    /// would go through when indexed (nothing is written).
    fn simulate_ingest(
        &self,
        index: Option<&str>,
        q: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let Some(req) = parse_json(body) else { return (400, super::malformed_body()) };
        let Some(docs) = req.get("docs").and_then(Value::as_array) else {
            return IngestError::parse("[docs] required property is missing", None, Some("docs"))
                .response();
        };
        let s = self.0.lock().unwrap();
        let subs = req.get("pipeline_substitutions").and_then(Value::as_object);
        let store =
            Store { pipelines: &s.admin.pipelines, substitutions: subs, scripts: &s.admin.scripts };
        let requested = q.get("pipeline").map(String::as_str);
        let mut out = Vec::new();
        for d in docs {
            let idx = d
                .get("_index")
                .map(java_string)
                .or_else(|| index.map(String::from))
                .unwrap_or_default();
            let id = d.get("_id").map(java_string);
            let src = d.get("_source").and_then(Value::as_object).cloned().unwrap_or_default();
            let r = run_write_pipelines(&s, &store, &idx, id.as_deref(), None, src, requested);
            let doc = match r {
                Ingested::Doc { index, id, source, executed, .. } => {
                    json!({"_id": id, "_index": index, "_version": -3, "_source": source,
                           "executed_pipelines": executed})
                }
                Ingested::Dropped => {
                    json!({"_index": idx, "_id": id, "_version": -3, "result": "noop",
                                            "_shards": {"total": 0, "successful": 0, "failed": 0}})
                }
                Ingested::Failed(e) if e.status == 500 && e.kind == "runtime_exception" => {
                    return e.response();
                }
                Ingested::Failed(e) => {
                    json!({"_id": id, "_index": idx, "error": Value::Object(e.body())})
                }
            };
            out.push(json!({"doc": doc}));
        }
        (200, json!({"docs": out}))
    }
}

fn truthy(q: &HashMap<String, String>, k: &str) -> bool {
    q.get(k).is_some_and(|v| v.is_empty() || v == "true")
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

    fn simulate(e: &Engine, processors: Value, source: Value) -> Value {
        let (_, r) = call(
            e,
            "POST",
            "/_ingest/pipeline/_simulate",
            json!({"pipeline": {"processors": processors}, "docs": [{"_source": source}]}),
        );
        r["docs"][0].clone()
    }

    #[test]
    fn pipeline_crud_and_validation() {
        let e = Engine::default();
        let p = json!({"description": "d", "processors": [{"set": {"field": "a", "value": 1}}], "version": 3});
        assert_eq!(call(&e, "PUT", "/_ingest/pipeline/p", p.clone()).0, 200);
        assert_eq!(call(&e, "GET", "/_ingest/pipeline/p", Value::Null).1, json!({"p": p}));
        assert_eq!(
            call(&e, "GET", "/_ingest/pipeline/p?summary=true", Value::Null).1,
            json!({"p": {}})
        );
        let (st, r) = call(&e, "PUT", "/_ingest/pipeline/q", json!({"processors": [{"nope": {}}]}));
        assert_eq!((st, r["error"]["type"].clone()), (400, json!("parse_exception")));
        let (st, r) = call(&e, "PUT", "/_ingest/pipeline/q", json!({"processors": [], "bad": 1}));
        assert_eq!((st, r["error"]["type"].clone()), (400, json!("parse_exception")));
        assert_eq!(call(&e, "DELETE", "/_ingest/pipeline/p", Value::Null).0, 200);
        assert_eq!(call(&e, "GET", "/_ingest/pipeline/p", Value::Null), (404, json!({})));
    }

    #[test]
    fn processors_transform_documents() {
        let e = Engine::default();
        let d = simulate(
            &e,
            json!([
                {"set": {"field": "x", "value": "{{a}}-y"}},
                {"lowercase": {"field": "name"}},
                {"rename": {"field": "old", "target_field": "new"}},
                {"convert": {"field": "n", "type": "integer"}},
                {"split": {"field": "csv", "separator": ","}},
                {"remove": {"field": "gone"}},
                {"append": {"field": "tags", "value": ["b"]}},
                {"gsub": {"field": "g", "pattern": "(\\d)", "replacement": "<$1>"}},
            ]),
            json!({"a": "A", "name": "BoB", "old": 1, "n": "42", "csv": "a,b,,", "gone": 1, "tags": "a", "g": "x1y2"}),
        );
        assert_eq!(
            d["doc"]["_source"],
            json!({"a": "A", "x": "A-y", "name": "bob", "new": 1, "n": 42, "csv": ["a", "b"], "tags": ["a", "b"], "g": "x<1>y<2>"})
        );
    }

    #[test]
    fn failures_conditions_and_on_failure() {
        let e = Engine::default();
        let d = simulate(&e, json!([{"lowercase": {"field": "zz"}}]), json!({"a": 1}));
        assert_eq!(d["error"]["reason"], json!("field [zz] not present as part of path [zz]"));
        let d = simulate(
            &e,
            json!([{"rename": {"field": "nope", "target_field": "y", "on_failure": [
                {"set": {"field": "err", "value": "{{_ingest.on_failure_message}}"}}]}}]),
            json!({}),
        );
        assert_eq!(d["doc"]["_source"]["err"], json!("field [nope] doesn't exist"));
        let d = simulate(
            &e,
            json!([{"set": {"field": "y", "value": 1, "if": "ctx.a == 2"}}]),
            json!({"a": 1}),
        );
        assert_eq!(d["doc"]["_source"], json!({"a": 1}));
        let d = simulate(
            &e,
            json!([{"grok": {"field": "m", "patterns": ["%{IP:client} %{WORD:verb} %{NUMBER:bytes:int}"]}}]),
            json!({"m": "1.2.3.4 GET 15"}),
        );
        assert_eq!(d["doc"]["_source"]["client"], json!("1.2.3.4"));
        assert_eq!(d["doc"]["_source"]["bytes"], json!(15));
        let d = simulate(&e, json!([{"script": {"source": "ctx.b = ctx.a * 2"}}]), json!({"a": 2}));
        assert_eq!(d["doc"]["_source"]["b"], json!(4));
    }

    #[test]
    fn writes_run_default_and_requested_pipelines() {
        let e = Engine::default();
        call(
            &e,
            "PUT",
            "/_ingest/pipeline/up",
            json!({"processors": [{"uppercase": {"field": "a"}}]}),
        );
        call(&e, "PUT", "/_ingest/pipeline/drop", json!({"processors": [{"drop": {}}]}));
        call(&e, "PUT", "/i", json!({"settings": {"default_pipeline": "up"}}));
        call(&e, "PUT", "/i/_doc/1", json!({"a": "x"}));
        assert_eq!(call(&e, "GET", "/i/_doc/1", Value::Null).1["_source"], json!({"a": "X"}));
        let (st, r) = call(&e, "PUT", "/i/_doc/2?pipeline=drop", json!({"a": "x"}));
        assert_eq!((st, r["result"].clone()), (200, json!("noop")));
        assert_eq!(call(&e, "GET", "/i/_doc/2", Value::Null).0, 404);
        let (st, r) = call(&e, "PUT", "/i/_doc/3?pipeline=missing", json!({"a": "x"}));
        assert_eq!((st, r["error"]["type"].clone()), (400, json!("illegal_argument_exception")));
        let (_, r) = call(
            &e,
            "POST",
            "/_ingest/_simulate",
            json!({"docs": [{"_index": "i", "_id": "1", "_source": {"a": "y"}}]}),
        );
        assert_eq!(r["docs"][0]["doc"]["executed_pipelines"], json!(["up"]));
        assert_eq!(r["docs"][0]["doc"]["_source"], json!({"a": "Y"}));
    }

    #[test]
    fn dissect_and_replacement_helpers() {
        let out = super::dissect("%{a} %{b} %{+a}", "1 2 3", " ").unwrap();
        assert_eq!(
            out,
            vec![("a".to_string(), "1 3".to_string()), ("b".to_string(), "2".to_string())]
        );
        assert_eq!(super::java_replacement("<$1>"), "<${1}>");
    }
}
