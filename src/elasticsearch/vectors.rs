//! `dense_vector` fields and kNN search: mapping defaults, validation and
//! update rules; document vector validation; exact nearest-neighbour search
//! (the top-level `knn` search option and the `knn` query) with
//! Elasticsearch's similarity-to-score transforms; and the vector functions
//! Painless scoring scripts call (`cosineSimilarity`, `dotProduct`, ...).
//!
//! Every search here is exact (a linear scan): real Elasticsearch searches
//! an approximate HNSW graph. For the document counts a local dev database
//! holds, exact search returns what the approximate one aims for and is
//! far simpler (see `docs/SERVICE_GUIDE.md`: "small and simple,
//! performance is not a goal"). The scores of a quantized field
//! (`int8_hnsw`, `int4_flat`, ...) are computed on the quantized vectors,
//! as Elasticsearch computes them (see `quantize`).

use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::engine::error;
use super::quantize;
use super::search::{CommittedDoc, EsError, InnerMatches, eval};

/// The most dimensions a `dense_vector` may have, and the most
/// `num_candidates` a kNN search may ask for.
const MAX_DIMS: u64 = 4096;
const MAX_CANDIDATES: u64 = 10_000;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Elem {
    Float,
    Byte,
    Bit,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Sim {
    L2,
    Cosine,
    Dot,
    Mip,
}

/// A mapped `dense_vector` field.
#[derive(Clone, Copy, Debug)]
pub struct Field {
    pub elem: Elem,
    /// `None` when the field isn't indexed (no kNN search on it).
    pub sim: Option<Sim>,
    /// Unset until the first document gives it, for a field mapped
    /// without `dims`. For `bit` vectors, the number of bits.
    pub dims: Option<usize>,
    /// How an `int8_*`/`int4_*` index quantizes the vectors its kNN
    /// search scores (see `quantize`).
    pub quant: Option<quantize::Options>,
}

impl Field {
    pub fn of(def: &Value) -> Option<Field> {
        if def.get("type").and_then(Value::as_str) != Some("dense_vector") {
            return None;
        }
        let elem = match def.get("element_type").and_then(Value::as_str) {
            Some("byte") => Elem::Byte,
            Some("bit") => Elem::Bit,
            _ => Elem::Float,
        };
        let indexed = !matches!(def.get("index"), Some(Value::Bool(false)))
            && def.get("index").and_then(Value::as_str) != Some("false");
        let sim = indexed.then(|| match def.get("similarity").and_then(Value::as_str) {
            Some("l2_norm") => Sim::L2,
            Some("dot_product") => Sim::Dot,
            Some("max_inner_product") => Sim::Mip,
            Some("cosine") => Sim::Cosine,
            _ if elem == Elem::Bit => Sim::L2,
            _ => Sim::Cosine,
        });
        let dims = def.get("dims").and_then(Value::as_u64).map(|d| d as usize);
        let options = def.get("index_options");
        let bits = match options.and_then(|o| o.get("type")).and_then(Value::as_str) {
            Some(t) if t.starts_with("int8") => Some(7),
            Some(t) if t.starts_with("int4") => Some(4),
            _ => None,
        };
        let quant = bits.filter(|_| sim.is_some() && elem == Elem::Float).map(|bits| {
            let confidence = options
                .and_then(|o| o.get("confidence_interval"))
                .and_then(Value::as_f64)
                .map(|c| c as f32);
            quantize::Options { bits, confidence }
        });
        Some(Field { elem, sim, dims, quant })
    }

    /// The dimensions a parsed vector of `len` values has (a `bit` vector
    /// is parsed as bytes, 8 dimensions each).
    fn dims_of(&self, len: usize) -> usize {
        if self.elem == Elem::Bit { len * 8 } else { len }
    }
}

/// A field's mapping by its dotted name (through object and nested
/// properties).
pub(crate) fn field_def<'a>(mappings: &'a Value, field: &str) -> Option<&'a Value> {
    let mut node = mappings;
    for seg in field.split('.') {
        node = node.get("properties")?.get(seg)?;
    }
    Some(node)
}

/// A document's (single) value for a dotted field name.
fn source_value<'a>(source: &'a Value, field: &str) -> Option<&'a Value> {
    let mut node = source;
    for seg in field.split('.') {
        node = node.get(seg)?;
    }
    (!node.is_null()).then_some(node)
}

// --- Number formatting and parsing ------------------------------------

/// A float as Java's `Float.toString` prints it (`3.0`, `0.6`, `1.0E-5`),
/// as Elasticsearch's messages show vector values.
pub(crate) fn java_float(f: f32) -> String {
    if f.is_infinite() {
        return if f > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    let a = f.abs();
    if a == 0.0 || (1e-3..1e7).contains(&a) {
        let s = format!("{f}");
        if s.contains('.') { s } else { format!("{s}.0") }
    } else {
        let s = format!("{f:e}");
        let (m, e) = s.split_once('e').unwrap_or((&s, "0"));
        let m = if m.contains('.') { m.to_string() } else { format!("{m}.0") };
        format!("{m}E{e}")
    }
}

/// `Preview of invalid vector: [...]` (the first ten values).
fn preview(v: &[f32]) -> String {
    let mut parts: Vec<String> = v.iter().take(10).map(|x| java_float(*x)).collect();
    if v.len() > 10 {
        parts.push("...".into());
    }
    format!("Preview of invalid vector: [{}]", parts.join(", "))
}

/// Hex-encoded bytes (`"807f0a"` is `[-128, 127, 10]`), as byte and bit
/// vectors may be given.
fn decode_hex(s: &str) -> Result<Vec<f32>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("string length not even: {}", s.len()));
    }
    let b = s.as_bytes();
    (0..b.len())
        .step_by(2)
        .map(|i| {
            let digit = |j: usize| {
                (b[j] as char).to_digit(16).ok_or_else(|| {
                    format!("Illegal hexadecimal character {} at index {j}", b[j] as char)
                })
            };
            Ok((digit(i)? * 16 + digit(i + 1)?) as u8 as i8 as f32)
        })
        .collect()
}

pub(crate) fn token_name(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "VALUE_STRING",
        Value::Number(_) => "VALUE_NUMBER",
        Value::Bool(_) => "VALUE_BOOLEAN",
        Value::Null => "VALUE_NULL",
        Value::Array(_) => "START_ARRAY",
        Value::Object(_) => "START_OBJECT",
    }
}

/// A byte/bit vector value outside what a signed byte holds: why.
fn byte_problem(x: f64, shown: &str, dim: usize) -> Option<String> {
    if x.fract() != 0.0 {
        Some(format!(
            "element_type [byte] vectors only support non-decimal values but found decimal value \
             [{shown}] at dim [{dim}];"
        ))
    } else if !(-128.0..=127.0).contains(&x) {
        Some(format!(
            "element_type [byte] vectors only support integers between [-128, 127] but found \
             [{shown}] at dim [{dim}];"
        ))
    } else {
        None
    }
}

fn magnitude_sq(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum()
}

/// What `similarity` refuses in a vector (indexed or queried): a zero
/// vector for `cosine`, a non-unit float vector for `dot_product`.
fn similarity_problem(f: &Field, v: &[f32]) -> Option<String> {
    let m = magnitude_sq(v);
    match f.sim? {
        Sim::Cosine if m == 0.0 => Some(format!(
            "The [cosine] similarity does not support vectors with zero magnitude. {}",
            preview(v)
        )),
        Sim::Dot if f.elem == Elem::Float && (m - 1.0).abs() > 1e-4 => Some(format!(
            "The [dot_product] similarity can only be used with unit-length vectors. {}",
            preview(v)
        )),
        _ => None,
    }
}

// --- Documents --------------------------------------------------------

/// Why a document's vector was refused: (cause type, message).
type Refusal = (&'static str, String);

fn unexpected(found: &str) -> Refusal {
    (
        "parsing_exception",
        format!(
            "Failed to parse object: expecting token of type [VALUE_NUMBER] but found [{found}]"
        ),
    )
}

/// A document's value for a `dense_vector` field, checked the way
/// Elasticsearch checks it while indexing.
fn doc_vector(f: &Field, name: &str, id: &str, v: &Value) -> Result<Vec<f32>, Refusal> {
    let values: Vec<f32> = match v {
        Value::String(s) if f.elem != Elem::Float => {
            decode_hex(s).map_err(|e| ("illegal_argument_exception", e))?
        }
        Value::Array(a) => {
            let per_vector = f.dims.map(|d| if f.elem == Elem::Bit { d.div_ceil(8) } else { d });
            let mut out = Vec::with_capacity(a.len());
            for (i, e) in a.iter().enumerate() {
                let Value::Number(n) = e else { return Err(unexpected(token_name(e))) };
                if f.elem != Elem::Bit
                    && let Some(d) = per_vector
                    && i >= d
                {
                    return Err((
                        "illegal_argument_exception",
                        format!(
                            "The [dense_vector] field [{name}] in doc [document with id '{id}'] \
                             has more dimensions than defined in the mapping [{d}]"
                        ),
                    ));
                }
                let x = n.as_f64().unwrap_or(0.0);
                if f.elem == Elem::Float {
                    let x = x as f32;
                    if x.is_infinite() {
                        let shown: Vec<f32> =
                            a.iter().filter_map(Value::as_f64).map(|x| x as f32).collect();
                        return Err((
                            "illegal_argument_exception",
                            format!(
                                "element_type [float] vectors do not support infinite values but \
                                 found [{}] at dim [{i}]; {}",
                                java_float(x),
                                preview(&shown)
                            ),
                        ));
                    }
                    out.push(x);
                } else {
                    if let Some(p) = byte_problem(x, &n.to_string(), i) {
                        return Err(("illegal_argument_exception", p));
                    }
                    out.push(x as f32);
                }
            }
            out
        }
        Value::Object(_) => return Err(unexpected("FIELD_NAME")),
        _ => return Err(unexpected("END_OBJECT")),
    };
    if let Some(d) = f.dims {
        let got = f.dims_of(values.len());
        if f.elem == Elem::Bit && got != d {
            return Err((
                "illegal_argument_exception",
                format!(
                    "The number of dimensions for field [{name}] should be [{d}] but found [{got}]"
                ),
            ));
        }
        if got != d {
            return Err((
                "illegal_argument_exception",
                format!(
                    "The [dense_vector] field [{name}] in doc [document with id '{id}'] has a \
                     different number of dimensions [{got}] than defined in the mapping [{d}]"
                ),
            ));
        }
    }
    if let Some(p) = similarity_problem(f, &values) {
        return Err(("illegal_argument_exception", p));
    }
    Ok(values)
}

fn refusal_error((kind, msg): Refusal) -> (u16, Value) {
    let full = format!("[1:1] failed to parse: {msg}");
    let root = if kind == "parsing_exception" {
        json!({"type": kind, "reason": msg})
    } else {
        json!({"type": "document_parsing_exception", "reason": full})
    };
    (
        400,
        json!({"error": {"root_cause": [root], "type": "document_parsing_exception",
                         "reason": full, "caused_by": {"type": kind, "reason": msg}},
               "status": 400}),
    )
}

/// Checks a document's `dense_vector` values against the mapping, and
/// records the dimensions of a field mapped without them (the first
/// document's vector sets them, as in Elasticsearch).
pub fn check_source(mappings: &mut Value, src: &Value, id: &str) -> Result<(), (u16, Value)> {
    fn walk(props: &mut Value, src: &Value, prefix: &str, id: &str) -> Result<(), Refusal> {
        let (Some(props), Some(fields)) = (props.as_object_mut(), src.as_object()) else {
            return Ok(());
        };
        for (k, v) in fields {
            let Some(def) = props.get_mut(k) else { continue };
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            if let Some(f) = Field::of(def) {
                if v.is_null() {
                    continue;
                }
                let vector = doc_vector(&f, &full, id, v)?;
                if f.dims.is_none() {
                    let dims = f.dims_of(vector.len());
                    let options = def.get("index_options").and_then(|o| o.get("type"));
                    if let Some(ty) =
                        options.and_then(Value::as_str).filter(|t| t.starts_with("int4"))
                        && !dims.is_multiple_of(2)
                    {
                        return Err((
                            "illegal_argument_exception",
                            format!("{ty} only supports even dimensions; provided={dims}"),
                        ));
                    }
                    def["dims"] = json!(dims);
                }
            } else if def.get("properties").is_some() {
                match v {
                    Value::Array(a) => {
                        for e in a {
                            walk(&mut def["properties"], e, &full, id)?;
                        }
                    }
                    _ => walk(&mut def["properties"], v, &full, id)?,
                }
            }
        }
        Ok(())
    }
    if let Some(props) = mappings.get_mut("properties") {
        walk(props, src, "", id).map_err(refusal_error)?;
    }
    super::features::check_source(mappings, src, id)
}

/// The mapping dynamic mapping gives an array: an array of 128 to 4096
/// floats is a `dense_vector` (Elasticsearch 8.11+), not a `float`.
pub fn dynamic_def(a: &[Value]) -> Option<Value> {
    let floats = a.first().is_some_and(Value::is_f64) && a.iter().all(Value::is_number);
    (floats && (128..=MAX_DIMS as usize).contains(&a.len())).then(|| {
        json!({"type": "dense_vector", "dims": a.len(), "index": true, "similarity": "cosine",
               "index_options": {"type": "int8_hnsw", "m": 16, "ef_construction": 100}})
    })
}

// --- Mappings ---------------------------------------------------------

const PARAMS: &[&str] =
    &["type", "element_type", "dims", "index", "similarity", "index_options", "meta"];
const SIMILARITIES: &[&str] = &["l2_norm", "cosine", "dot_product", "max_inner_product"];
const OPTION_TYPES: &[&str] = &["hnsw", "int8_hnsw", "int4_hnsw", "flat", "int8_flat", "int4_flat"];

fn mapping_error(reason: &str) -> (u16, Value) {
    let mut e =
        error("mapper_parsing_exception", &format!("Failed to parse mapping: {reason}"), 400);
    e["error"]["caused_by"] = json!({"type": "mapper_parsing_exception", "reason": reason});
    (400, e)
}

/// A `dense_vector` definition with the defaults GET `_mapping` shows
/// filled in, or why Elasticsearch refuses it.
fn normalize(name: &str, def: &Value) -> Result<Value, String> {
    let o = def.as_object().cloned().unwrap_or_default();
    if let Some(k) = o.keys().find(|k| !PARAMS.contains(&k.as_str())) {
        return Err(format!("unknown parameter [{k}] on mapper [{name}] of type [dense_vector]"));
    }
    let elem = match o.get("element_type") {
        None => "float",
        Some(v) => match v.as_str() {
            Some(e @ ("float" | "byte" | "bit")) => e,
            _ => {
                let shown = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                return Err(format!(
                    "invalid element_type [{shown}]; available types are [byte, float, bit]"
                ));
            }
        },
    };
    let dims = match o.get("dims") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let n = v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())).ok_or_else(
                || {
                    let shown = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                    format!(
                        "Property [dims] on field [{name}] must be an integer but got [{shown}]"
                    )
                },
            )?;
            if !(1..=MAX_DIMS as i64).contains(&n) {
                return Err(format!(
                    "The number of dimensions for field [{name}] should be in the range [1, {MAX_DIMS}] but was [{n}]"
                ));
            }
            Some(n)
        }
    };
    let indexed = match o.get("index") {
        None => true,
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) if s == "true" || s == "false" => s == "true",
        Some(v) => {
            let shown = v.as_str().map_or_else(|| v.to_string(), str::to_string);
            return Err(format!(
                "Failed to parse value [{shown}] as only [true] or [false] are allowed."
            ));
        }
    };
    let mut out = Map::new();
    out.insert("type".into(), json!("dense_vector"));
    if o.contains_key("element_type") && elem != "float" {
        out.insert("element_type".into(), json!(elem));
    }
    if let Some(d) = dims {
        out.insert("dims".into(), json!(d));
    }
    out.insert("index".into(), json!(indexed));
    if let Some(m) = o.get("meta") {
        out.insert("meta".into(), m.clone());
    }
    if !indexed {
        for p in ["similarity", "index_options"] {
            if o.contains_key(p) {
                return Err(format!(
                    "Field [{p}] can only be specified for a field of type [dense_vector] when it is indexed"
                ));
            }
        }
        return Ok(Value::Object(out));
    }
    let sim = match o.get("similarity") {
        None => {
            if elem == "bit" {
                "l2_norm"
            } else {
                "cosine"
            }
        }
        Some(v) => match v.as_str().filter(|s| SIMILARITIES.contains(s)) {
            Some(s) => s,
            None => {
                let shown = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                return Err(format!(
                    "Unknown value [{shown}] for field [similarity] - accepted values are [l2_norm, cosine, dot_product, max_inner_product]"
                ));
            }
        },
    };
    if elem == "bit" && sim != "l2_norm" {
        return Err(
            "The [l2_norm] similarity is the only supported similarity for bit vectors".into()
        );
    }
    out.insert("similarity".into(), json!(sim));
    match o.get("index_options") {
        Some(opts) => {
            out.insert("index_options".into(), index_options(name, elem, dims, opts)?);
        }
        // Float vectors are int8-quantized by default (8.14+).
        None if elem == "float" => {
            out.insert(
                "index_options".into(),
                json!({"type": "int8_hnsw", "m": 16, "ef_construction": 100}),
            );
        }
        None => {}
    }
    Ok(Value::Object(out))
}

fn index_options(name: &str, elem: &str, dims: Option<i64>, opts: &Value) -> Result<Value, String> {
    let o = opts.as_object().cloned().unwrap_or_default();
    let ty = match o.get("type") {
        None => return Err("[index_options] requires field [type] to be configured".into()),
        Some(v) => v.as_str().map_or_else(|| v.to_string(), str::to_string),
    };
    if !OPTION_TYPES.contains(&ty.as_str()) {
        return Err(format!("Unknown vector index options type [{ty}] for field [{name}]"));
    }
    let quantized = ty.starts_with("int");
    let hnsw = ty.ends_with("hnsw");
    let allowed: &[&str] = match (hnsw, quantized) {
        (true, true) => &["type", "m", "ef_construction", "confidence_interval"],
        (true, false) => &["type", "m", "ef_construction"],
        (false, true) => &["type", "confidence_interval"],
        (false, false) => &["type"],
    };
    let unknown: Vec<String> = o
        .iter()
        .filter(|(k, _)| !allowed.contains(&k.as_str()))
        .map(|(k, v)| format!("{k} : {}", v.as_str().map_or_else(|| v.to_string(), str::to_string)))
        .collect();
    if !unknown.is_empty() {
        return Err(format!(
            "Mapping definition for [{name}] has unsupported parameters:  [{}]",
            unknown.join(", ")
        ));
    }
    if quantized && elem != "float" {
        return Err(format!("[element_type] cannot be [{elem}] when using index type [{ty}]"));
    }
    if ty.starts_with("int4")
        && let Some(d) = dims
        && d % 2 != 0
    {
        return Err(format!("{ty} only supports even dimensions; provided={d}"));
    }
    let int = |k: &str, default: i64| -> Result<i64, String> {
        match o.get(k) {
            None => Ok(default),
            Some(v) => {
                v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())).ok_or_else(|| {
                    format!(
                        "For input string: \"{}\"",
                        v.as_str().map_or_else(|| v.to_string(), str::to_string)
                    )
                })
            }
        }
    };
    let mut out = Map::new();
    out.insert("type".into(), json!(ty));
    if hnsw {
        out.insert("m".into(), json!(int("m", 16)?));
        out.insert("ef_construction".into(), json!(int("ef_construction", 100)?));
    }
    if quantized {
        match o.get("confidence_interval").and_then(Value::as_f64) {
            Some(c) => {
                out.insert("confidence_interval".into(), json!(c));
            }
            // int4 shows its dynamic default.
            None if ty.starts_with("int4") => {
                out.insert("confidence_interval".into(), json!(0.0));
            }
            None => {}
        }
    }
    Ok(Value::Object(out))
}

/// `index_options` as Elasticsearch's conflict messages print them.
fn options_shown(o: &Value) -> String {
    let ty = o.get("type").and_then(Value::as_str).unwrap_or("hnsw");
    let mut parts = vec![format!("type={ty}")];
    if ty.ends_with("hnsw") {
        parts.push(format!("m={}", o.get("m").unwrap_or(&json!(16))));
        parts.push(format!("ef_construction={}", o.get("ef_construction").unwrap_or(&json!(100))));
    }
    if ty.starts_with("int") {
        parts.push(format!(
            "confidence_interval={}",
            o.get("confidence_interval")
                .map_or("null".to_string(), |c| format!("{:?}", c.as_f64().unwrap_or(0.0)))
        ));
    }
    format!("{{{}}}", parts.join(", "))
}

/// Whether `old` index options may become `new` (only toward a "better"
/// index: flat to anything, int8_flat to an HNSW type, and an HNSW type to
/// itself or int8 with no fewer connections `m`).
fn options_updatable(old: &Value, new: &Value) -> bool {
    let ty = |o: &Value| o.get("type").and_then(Value::as_str).unwrap_or("hnsw").to_string();
    let m = |o: &Value| o.get("m").and_then(Value::as_i64).unwrap_or(16);
    let (ot, nt) = (ty(old), ty(new));
    match ot.as_str() {
        "flat" => true,
        "int8_flat" => matches!(nt.as_str(), "int8_flat" | "hnsw" | "int8_hnsw"),
        "int4_flat" => nt == "int4_flat",
        "hnsw" => matches!(nt.as_str(), "hnsw" | "int8_hnsw") && m(new) >= m(old),
        _ => nt == ot && m(new) >= m(old),
    }
}

/// The conflicts of updating `old` to `new`: "Cannot update parameter ...".
fn update_conflicts(old: &Value, new: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let shown = |v: Option<&Value>, default: &str| match v {
        None | Some(Value::Null) => default.to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
    };
    let mut param = |p: &str, a: String, b: String| {
        if a != b {
            out.push(format!("Cannot update parameter [{p}] from [{a}] to [{b}]"));
        }
    };
    param(
        "element_type",
        shown(old.get("element_type"), "float"),
        shown(new.get("element_type"), "float"),
    );
    if old.get("dims").is_some_and(|d| !d.is_null()) {
        param("dims", shown(old.get("dims"), "null"), shown(new.get("dims"), "null"));
    }
    param("index", shown(old.get("index"), "true"), shown(new.get("index"), "true"));
    param("similarity", shown(old.get("similarity"), "null"), shown(new.get("similarity"), "null"));
    let default = json!({"type": "hnsw", "m": 16, "ef_construction": 100});
    let (oo, no) = (
        old.get("index_options").unwrap_or(&default),
        new.get("index_options").unwrap_or(&default),
    );
    if new.get("index") != Some(&json!(false)) && !options_updatable(oo, no) {
        out.push(format!(
            "Cannot update parameter [index_options] from [{}] to [{}]",
            options_shown(oo),
            options_shown(no)
        ));
    }
    out
}

/// Validates the `dense_vector` fields of an incoming mapping (and, against
/// `current`, their updates), filling in the defaults Elasticsearch shows.
pub fn prepare_mapping(current: &Value, incoming: &mut Value) -> Result<(), (u16, Value)> {
    super::features::check_mapping(incoming)?;
    fn walk(cur: Option<&Value>, inc: &mut Value, prefix: &str) -> Result<(), (u16, Value)> {
        let Some(props) = inc.get_mut("properties").and_then(Value::as_object_mut) else {
            return Ok(());
        };
        for (k, def) in props.iter_mut() {
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            let existing = cur.and_then(|c| c.get("properties")).and_then(|p| p.get(k));
            if def.get("type").and_then(Value::as_str) == Some("dense_vector") {
                let next = normalize(k, def).map_err(|e| mapping_error(&e))?;
                if let Some(old) = existing.filter(|e| Field::of(e).is_some()) {
                    let conflicts = update_conflicts(old, &next);
                    if !conflicts.is_empty() {
                        return Err((
                            400,
                            error(
                                "illegal_argument_exception",
                                &format!(
                                    "Mapper for [{full}] conflicts with existing mapper:\n\t{}",
                                    conflicts.join("\n\t")
                                ),
                                400,
                            ),
                        ));
                    }
                }
                *def = next;
            } else {
                walk(existing, def, &full)?;
            }
        }
        Ok(())
    }
    walk(Some(current), incoming, "")?;
    // Dynamic templates mapping vectors are checked up front too.
    if let Some(templates) = incoming.get("dynamic_templates").and_then(Value::as_array) {
        for t in templates.iter().filter_map(Value::as_object).flat_map(|o| o.values()) {
            if let Some(m) = t.get("mapping")
                && m.get("type").and_then(Value::as_str) == Some("dense_vector")
            {
                normalize("_dynamic", m).map_err(|e| mapping_error(&e))?;
            }
        }
    }
    Ok(())
}

// --- Scoring ----------------------------------------------------------

/// The `_score` of document vector `v` for query vector `q`, as Lucene
/// scores each similarity (always non-negative, higher is closer).
fn score(f: &Field, q: &[f32], v: &[f32]) -> f32 {
    if f.elem == Elem::Bit {
        let ham: u32 =
            q.iter().zip(v).map(|(a, b)| ((*a as i8 as u8) ^ (*b as i8 as u8)).count_ones()).sum();
        let bits = (q.len() * 8) as f32;
        return (bits - ham as f32) / bits;
    }
    let dot: f32 = q.iter().zip(v).map(|(a, b)| a * b).sum();
    match (f.sim.unwrap_or(Sim::Cosine), f.elem) {
        (Sim::L2, _) => {
            let d: f32 = q.iter().zip(v).map(|(a, b)| (a - b) * (a - b)).sum();
            1.0 / (1.0 + d)
        }
        (Sim::Cosine, Elem::Float) => {
            // Elasticsearch stores and queries cosine vectors normalized.
            let (qm, vm) = (magnitude_sq(q).sqrt(), magnitude_sq(v).sqrt());
            let d: f32 = q.iter().zip(v).map(|(a, b)| (a / qm) * (b / vm)).sum();
            ((1.0 + d) / 2.0).max(0.0)
        }
        (Sim::Cosine, _) => {
            let cos = dot as f64 / (magnitude_sq(q) as f64 * magnitude_sq(v) as f64).sqrt();
            (1.0 + cos as f32) / 2.0
        }
        (Sim::Dot, Elem::Float) => ((1.0 + dot) / 2.0).max(0.0),
        (Sim::Dot, _) => 0.5 + dot / (q.len() as f32 * 32768.0),
        (Sim::Mip, _) => {
            if dot < 0.0 {
                1.0 / (1.0 - dot)
            } else {
                dot + 1.0
            }
        }
    }
}

/// The lowest `_score` a kNN `similarity` threshold lets through (the
/// threshold is in the similarity's own terms: a distance for `l2_norm`,
/// a cosine for `cosine`, ...).
fn min_score(f: &Field, s: f32, dims: usize) -> f32 {
    if f.elem == Elem::Bit {
        return (dims as f32 - s) / dims as f32;
    }
    match f.sim.unwrap_or(Sim::Cosine) {
        Sim::L2 => 1.0 / (1.0 + s * s),
        Sim::Cosine => (1.0 + s) / 2.0,
        Sim::Dot if f.elem == Elem::Byte => 0.5 + s / (dims as f32 * 32768.0),
        Sim::Dot => (1.0 + s) / 2.0,
        Sim::Mip => {
            if s < 0.0 {
                1.0 / (1.0 - s)
            } else {
                s + 1.0
            }
        }
    }
}

// --- kNN --------------------------------------------------------------

const QUERY_KEYS: &[&str] = &[
    "field",
    "query_vector",
    "query_vector_builder",
    "k",
    "num_candidates",
    "filter",
    "similarity",
    "boost",
    "_name",
];

/// A parsed `knn` query.
struct Knn {
    field: String,
    query: Vec<f32>,
    k: usize,
    filters: Vec<Value>,
    similarity: Option<f32>,
    boost: f32,
}

fn parse_failure(reason: &str) -> EsError {
    EsError::new(
        400,
        "x_content_parse_exception",
        "Failed to build [knn] after last required field arrived",
    )
    .caused_by("illegal_argument_exception", reason)
}

/// `query_vector`: numbers, or a hex string of signed bytes.
fn parse_query_vector(v: &Value) -> Result<Vec<f32>, EsError> {
    let bad = |kind: &str, reason: &str| {
        EsError::new(400, "x_content_parse_exception", "[knn] failed to parse field [query_vector]")
            .caused_by(kind, reason)
    };
    match v {
        Value::Array(a) => a
            .iter()
            .map(|e| match e {
                Value::Number(n) => Ok(n.as_f64().unwrap_or(0.0) as f32),
                Value::String(s) => match s.parse::<f32>() {
                    Ok(x) => Ok(x),
                    Err(_) => {
                        Err(bad("number_format_exception", &format!("For input string: \"{s}\"")))
                    }
                },
                other => Err(bad(
                    "illegal_argument_exception",
                    &format!("expected a number but found [{}]", token_name(other)),
                )),
            })
            .collect(),
        Value::String(s) => decode_hex(s).map_err(|e| bad("illegal_argument_exception", &e)),
        other => Err(bad(
            "illegal_argument_exception",
            &format!("expected an array or a string but found [{}]", token_name(other)),
        )),
    }
}

fn filters_of(v: Option<&Value>) -> Vec<Value> {
    match v {
        Some(Value::Array(a)) => a.clone(),
        Some(Value::Null) | None => Vec::new(),
        Some(q) => vec![q.clone()],
    }
}

fn number(v: Option<&Value>) -> Option<f64> {
    v.and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

/// The `knn` query (query DSL form).
fn parse_query(v: &Value) -> Result<Knn, EsError> {
    let Some(o) = v.as_object() else {
        return Err(EsError::parsing("[knn] query malformed, no start_object after query name"));
    };
    if let Some(k) = o.keys().find(|k| !QUERY_KEYS.contains(&k.as_str())) {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            &format!("[knn] unknown field [{k}]"),
        ));
    }
    let Some(field) = o.get("field").and_then(Value::as_str) else {
        return Err(EsError::new(400, "illegal_argument_exception", "Required [field]"));
    };
    let query = match (o.get("query_vector"), o.get("query_vector_builder")) {
        (Some(_), Some(_)) => {
            return Err(parse_failure(
                "cannot provide both [query_vector_builder] and [query_vector]",
            ));
        }
        (None, None) => {
            return Err(parse_failure(
                "either [query_vector] or [query_vector_builder] must be provided",
            ));
        }
        (None, Some(_)) => return Err(no_inference()),
        (Some(q), None) => parse_query_vector(q)?,
    };
    let k = number(o.get("k")).map(|k| k as i64);
    let candidates = number(o.get("num_candidates")).map(|n| n as i64);
    if k.is_some_and(|k| k < 1) {
        return Err(parse_failure("[k] must be greater than 0"));
    }
    if candidates.is_some_and(|n| n > MAX_CANDIDATES as i64) {
        return Err(parse_failure("[num_candidates] cannot exceed [10000]"));
    }
    if let (Some(k), Some(n)) = (k, candidates)
        && n < k
    {
        return Err(parse_failure("[num_candidates] cannot be less than [k]"));
    }
    // Without `k`, as many neighbours as candidates; without either, the
    // search fills in `num_candidates` from its `size` (see
    // `fill_defaults`), 10 by default.
    let k = k.or(candidates).unwrap_or(15);
    if k < 1 {
        return Err(EsError::shard_failure(
            "query_shard_exception",
            &format!("failed to create query: k must be at least 1, got: {k}"),
        )
        .caused_by("illegal_argument_exception", &format!("k must be at least 1, got: {k}")));
    }
    Ok(Knn {
        field: field.to_string(),
        query,
        k: k as usize,
        filters: filters_of(o.get("filter")),
        similarity: number(o.get("similarity")).map(|s| s as f32),
        boost: number(o.get("boost")).unwrap_or(1.0) as f32,
    })
}

/// A `query_vector_builder` needs an inference model, which noida doesn't
/// have (neither does a real node without machine learning).
pub(crate) fn no_inference() -> EsError {
    EsError::new(
        500,
        "illegal_state_exception",
        "failed to find action [cluster:internal/xpack/ml/coordinatedinference] to execute",
    )
}

fn create_failure(reason: &str) -> EsError {
    EsError::shard_failure("query_shard_exception", &format!("failed to create query: {reason}"))
        .caused_by("illegal_argument_exception", reason)
}

/// The field a kNN search runs on, with the query vector checked against
/// it.
fn resolve(knn: &Knn, mappings: &Value) -> Result<Field, EsError> {
    let Some(def) = field_def(mappings, &knn.field) else {
        return Err(create_failure(&format!(
            "field [{}] does not exist in the mapping",
            knn.field
        )));
    };
    let Some(f) = Field::of(def) else {
        return Err(create_failure("[knn] queries are only supported on [dense_vector] fields"));
    };
    if f.sim.is_none() {
        return Err(create_failure(&format!(
            "to perform knn search on field [{}], its mapping must have [index] set to [true]",
            knn.field
        )));
    }
    if f.elem != Elem::Float {
        for (i, x) in knn.query.iter().enumerate() {
            if let Some(p) = byte_problem(*x as f64, &java_float(*x), i) {
                return Err(create_failure(&format!("{p} {}", preview(&knn.query))));
            }
        }
    }
    if let Some(d) = f.dims {
        let got = f.dims_of(knn.query.len());
        if got != d {
            return Err(create_failure(&format!(
                "The query vector has a different number of dimensions [{got}] than the document vectors [{d}]."
            )));
        }
    }
    if let Some(p) = similarity_problem(&f, &knn.query) {
        return Err(create_failure(&p));
    }
    Ok(f)
}

/// The documents passing every `filter` (all of them without one).
fn allowed(
    knn: &Knn,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Option<HashSet<usize>>, EsError> {
    let mut allowed: Option<HashSet<usize>> = None;
    for q in &knn.filters {
        let m: HashSet<usize> = eval(q, mappings, docs)?.into_keys().collect();
        allowed = Some(match allowed {
            Some(a) => a.intersection(&m).copied().collect(),
            None => m,
        });
    }
    Ok(allowed)
}

/// Each scored document's (index, score), filtered by the `similarity`
/// threshold; documents without a vector don't match. A quantized field
/// scores the quantized vectors, each index's as one segment.
fn scored(knn: &Knn, f: &Field, docs: &[CommittedDoc]) -> Vec<(usize, f32)> {
    let threshold = knn.similarity.map(|s| min_score(f, s, f.dims.unwrap_or(0)));
    let vectors: Vec<(usize, Vec<f32>)> = docs
        .iter()
        .enumerate()
        .filter_map(|(i, d)| {
            let v = source_value(&d.source, &knn.field)?;
            let v = doc_vector(f, &knn.field, &d.id, v).ok()?;
            (v.len() == knn.query.len()).then_some((i, v))
        })
        .collect();
    let mut scores: Vec<f32> = vectors.iter().map(|(_, v)| score(f, &knn.query, v)).collect();
    if let (Some(opts), Some(sim)) = (f.quant, f.sim) {
        let mut segments: Vec<(&str, Vec<usize>)> = Vec::new();
        for (n, (i, _)) in vectors.iter().enumerate() {
            let index = docs[*i].index.as_str();
            match segments.iter_mut().find(|(name, _)| *name == index) {
                Some((_, members)) => members.push(n),
                None => segments.push((index, vec![n])),
            }
        }
        for (_, members) in segments {
            let vs: Vec<&[f32]> = members.iter().map(|n| vectors[*n].1.as_slice()).collect();
            for (n, s) in members.iter().zip(quantize::scores(opts, sim, &knn.query, &vs)) {
                scores[*n] = s;
            }
        }
    }
    vectors
        .iter()
        .zip(scores)
        .filter(|(_, s)| threshold.is_none_or(|t| *s >= t))
        .map(|((i, _), s)| (*i, s * knn.boost))
        .collect()
}

fn by_score(a: &(usize, f32), b: &(usize, f32)) -> Ordering {
    b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0))
}

/// The `knn` query: the `k` nearest documents (that pass its filters).
pub fn eval_knn(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, f32>, EsError> {
    let knn = parse_query(v)?;
    let f = resolve(&knn, mappings)?;
    let allowed = allowed(&knn, mappings, docs)?;
    let mut hits = scored(&knn, &f, docs);
    if let Some(a) = &allowed {
        hits.retain(|(i, _)| a.contains(i));
    }
    hits.sort_by(by_score);
    // Among nested objects (a knn query inside a `nested` one, combined
    // with others), only the nearest object of each parent is a candidate.
    let nested = super::search::nested_paths(mappings)
        .iter()
        .any(|p| knn.field.starts_with(&format!("{p}.")));
    if nested {
        let mut seen = HashSet::new();
        hits.retain(|(i, _)| seen.insert((&docs[*i].index, &docs[*i].id)));
    }
    hits.truncate(knn.k);
    Ok(hits.into_iter().collect())
}

/// Whether a query is a `knn` query.
pub fn is_knn(q: &Value) -> bool {
    q.as_object().is_some_and(|o| o.len() == 1 && o.contains_key("knn"))
}

/// A `knn` query on a nested field (inside `nested`): the `k` nearest
/// parent documents, each scored by its nearest nested vector, with every
/// nested object within the `similarity` threshold as its matches (the
/// `inner_hits`). Its `filter` applies to the parents.
pub fn nested_knn(
    v: &Value,
    mappings: &Value,
    parents: &[CommittedDoc],
    children: &[CommittedDoc],
    owners: &[(usize, usize)],
) -> Result<InnerMatches, EsError> {
    let knn = parse_query(v)?;
    let f = resolve(&knn, mappings)?;
    let allowed = allowed(&knn, mappings, parents)?;
    let mut per: InnerMatches = HashMap::new();
    for (ci, s) in scored(&knn, &f, children) {
        let (parent, offset) = owners[ci];
        if allowed.as_ref().is_none_or(|a| a.contains(&parent)) {
            per.entry(parent).or_default().push((offset, s, ci));
        }
    }
    let mut best: Vec<(usize, f32)> =
        per.iter().map(|(p, ms)| (*p, ms.iter().map(|m| m.1).fold(f32::MIN, f32::max))).collect();
    best.sort_by(by_score);
    let keep: HashSet<usize> = best.iter().take(knn.k).map(|b| b.0).collect();
    per.retain(|p, _| keep.contains(p));
    Ok(per)
}

/// Fills in `num_candidates` for every `knn` query in `query` that gives
/// neither it nor `k`: one and a half times the search's `size`.
pub fn fill_defaults(query: &mut Value, size: i64) {
    match query {
        Value::Object(o) => {
            for (k, v) in o.iter_mut() {
                if k == "knn"
                    && let Some(spec) = v.as_object_mut()
                    && spec.contains_key("field")
                    && !spec.contains_key("k")
                    && !spec.contains_key("num_candidates")
                {
                    let n = ((size as f64) * 1.5).round().min(MAX_CANDIDATES as f64);
                    spec.insert("num_candidates".into(), json!(n as i64));
                }
                fill_defaults(v, size);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|v| fill_defaults(v, size)),
        _ => {}
    }
}

const SEARCH_KEYS: &[&str] = &[
    "field",
    "query_vector",
    "query_vector_builder",
    "k",
    "num_candidates",
    "filter",
    "similarity",
    "boost",
    "_name",
    "inner_hits",
];

/// The search's top-level `knn` option (one search or several) as a
/// query: each search becomes a `knn` query for its `k` nearest (in a
/// `nested` query, scored by the best nested vector, for a nested field),
/// combined with the request's `query` in a `bool` `should`, so a document
/// scores the sum of what matched it, as in Elasticsearch.
pub fn top_level_query(
    body: &Value,
    mappings: &Value,
    size: i64,
) -> Result<Option<Value>, EsError> {
    let Some(spec) = body.get("knn") else { return Ok(None) };
    let specs: Vec<&Value> = match spec {
        Value::Object(_) => vec![spec],
        Value::Array(a) => a.iter().collect(),
        other => {
            return Err(EsError::parsing(&format!(
                "Unknown key for a {} in [knn].",
                token_name(other)
            )));
        }
    };
    let mut clauses = Vec::new();
    for s in specs {
        let Some(o) = s.as_object() else {
            return Err(EsError::parsing(&format!(
                "Unknown key for a {} in [knn].",
                token_name(s)
            )));
        };
        if let Some(k) = o.keys().find(|k| !SEARCH_KEYS.contains(&k.as_str())) {
            return Err(EsError::new(
                400,
                "x_content_parse_exception",
                &format!("[knn] unknown field [{k}]"),
            ));
        }
        let illegal = |r: &str| EsError::new(400, "illegal_argument_exception", r);
        let Some(field) = o.get("field").and_then(Value::as_str) else {
            return Err(illegal("Required [field]"));
        };
        let query = match (o.get("query_vector"), o.get("query_vector_builder")) {
            (Some(_), Some(_)) => {
                return Err(illegal(
                    "cannot provide both [query_vector_builder] and [query_vector]",
                ));
            }
            (None, None) => {
                return Err(illegal(
                    "either [query_vector_builder] or [query_vector] must be provided",
                ));
            }
            (None, Some(_)) => return Err(no_inference()),
            (Some(q), None) => parse_query_vector(q)?,
        };
        let k = number(o.get("k")).map_or(size, |k| k as i64);
        if k < 1 {
            return Err(illegal("[k] must be greater than 0"));
        }
        let candidates = number(o.get("num_candidates")).map_or_else(
            || ((k as f64) * 1.5).min(MAX_CANDIDATES as f64).round() as i64,
            |n| n as i64,
        );
        if candidates > MAX_CANDIDATES as i64 {
            return Err(illegal("[num_candidates] cannot exceed [10000]"));
        }
        if candidates < k {
            return Err(illegal("[num_candidates] cannot be less than [k]"));
        }
        let mut q =
            json!({"field": field, "query_vector": query, "k": k, "num_candidates": candidates});
        for key in ["filter", "similarity", "boost", "_name"] {
            if let Some(v) = o.get(key) {
                q[key] = v.clone();
            }
        }
        let path = super::search::nested_paths(mappings)
            .into_iter()
            .filter(|p| field.starts_with(&format!("{p}.")))
            .max_by_key(String::len);
        let clause = match path {
            Some(path) => {
                let mut n = json!({"path": path, "query": {"knn": q}, "score_mode": "max"});
                if let Some(ih) = o.get("inner_hits") {
                    n["inner_hits"] = ih.clone();
                }
                json!({"nested": n})
            }
            None => json!({"knn": q}),
        };
        clauses.push(clause);
    }
    if let Some(q) = body.get("query") {
        clauses.push(q.clone());
    }
    Ok(Some(if clauses.len() == 1 {
        clauses.pop().unwrap_or_default()
    } else {
        json!({"bool": {"should": clauses}})
    }))
}

/// A query clause on a `dense_vector` field that only `knn` and
/// `exists` queries support: the error Elasticsearch gives.
pub fn unsupported_query(query: &Map<String, Value>, mappings: &Value) -> Option<EsError> {
    let (kind, spec) = query.iter().next()?;
    let field = match kind.as_str() {
        "term"
        | "terms"
        | "match"
        | "match_phrase"
        | "match_phrase_prefix"
        | "range"
        | "prefix"
        | "wildcard"
        | "regexp"
        | "fuzzy" => spec.as_object()?.keys().find(|k| !matches!(k.as_str(), "boost" | "_name"))?,
        _ => return None,
    };
    field_def(mappings, field).and_then(Field::of)?;
    let reason = match kind.as_str() {
        "term" | "terms" => {
            format!("Field [{field}] of type [dense_vector] doesn't support term queries")
        }
        "range" => format!("Field [{field}] of type [dense_vector] does not support range queries"),
        "prefix" | "wildcard" => format!(
            "Can only use {kind} queries on keyword, text and wildcard fields - not on [{field}] which is of type [dense_vector]"
        ),
        "regexp" | "fuzzy" => format!(
            "Can only use {kind} queries on keyword and text fields - not on [{field}] which is of type [dense_vector]"
        ),
        _ => format!("Field [{field}] of type [dense_vector] does not support match queries"),
    };
    Some(create_failure(&reason))
}

/// A `dense_vector` field named by an aggregation (or `docvalue_fields`):
/// the error Elasticsearch gives.
pub fn unsupported_doc_values(body: &Value, mappings: &Value) -> Option<EsError> {
    fn fields<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
        match v {
            Value::Object(o) => {
                if let Some(f) = o.get("field").and_then(Value::as_str) {
                    out.push(f);
                }
                o.values().for_each(|x| fields(x, out));
            }
            Value::Array(a) => a.iter().for_each(|x| fields(x, out)),
            _ => {}
        }
    }
    let mut names = Vec::new();
    if let Some(a) = body.get("aggs").or_else(|| body.get("aggregations")) {
        fields(a, &mut names);
    }
    match body.get("docvalue_fields") {
        Some(Value::Array(a)) => {
            for x in a {
                match x {
                    Value::String(s) => names.push(s),
                    other => fields(other, &mut names),
                }
            }
        }
        Some(Value::String(s)) => names.push(s),
        _ => {}
    }
    // Feature fields refuse sorting too.
    let mut sorted: Vec<&str> = Vec::new();
    let sorts = match body.get("sort") {
        Some(Value::Array(a)) => a.iter().collect(),
        Some(s) => vec![s],
        None => Vec::new(),
    };
    for s in sorts {
        match s {
            Value::String(f) => sorted.push(f),
            Value::Object(o) => sorted.extend(o.keys().map(String::as_str)),
            _ => {}
        }
    }
    let feature = names.iter().chain(&sorted).find_map(|f| {
        let ty = field_def(mappings, f)?.get("type")?.as_str()?;
        matches!(ty, "rank_feature" | "rank_features" | "sparse_vector").then_some(ty)
    });
    if let Some(ty) = feature {
        return Some(EsError::shard_failure(
            "illegal_argument_exception",
            &format!("[{ty}] fields do not support sorting, scripting or aggregating"),
        ));
    }
    let f = names.into_iter().find(|f| field_def(mappings, f).and_then(Field::of).is_some())?;
    Some(EsError::shard_failure(
        "illegal_argument_exception",
        &format!(
            "Field [{f}] of type [dense_vector] doesn't support docvalue_fields or aggregations"
        ),
    ))
}

// --- Script functions -------------------------------------------------

/// The `doc['field']` view of a vector for scoring scripts.
pub fn doc_view(def: &Value, source: &Value, field: &str) -> Option<Value> {
    let f = Field::of(def)?;
    let v = source_value(source, field).and_then(|v| doc_vector(&f, field, "", v).ok());
    let element = match f.elem {
        Elem::Float => "float",
        Elem::Byte => "byte",
        Elem::Bit => "bit",
    };
    Some(match v {
        Some(v) => json!({
            "vectorValue": v,
            "magnitude": magnitude_sq(&v).sqrt(),
            "dims": f.dims_of(v.len()),
            "empty": false,
            "length": 1,
            "__vector": element,
        }),
        None => json!({"empty": true, "length": 0, "__vector": element}),
    })
}

/// A Painless vector function (`cosineSimilarity(params.query_vector,
/// 'field')`, ...) on a document vector from `doc_view`.
pub fn script_function(name: &str, query: &Value, doc: &Value) -> Result<f64, String> {
    let Some(v) = doc.get("vectorValue").and_then(Value::as_array) else {
        return Err("A document doesn't have a value for a vector field!".into());
    };
    let v: Vec<f64> = v.iter().filter_map(Value::as_f64).collect();
    let q: Vec<f64> = match query {
        Value::Array(a) => a.iter().filter_map(Value::as_f64).collect(),
        Value::String(s) => decode_hex(s)?.into_iter().map(f64::from).collect(),
        _ => return Err("query vector must be a list of numbers or a hex string".into()),
    };
    let element = doc.get("__vector").and_then(Value::as_str).unwrap_or("float");
    if q.len() != v.len() {
        let (qd, vd) =
            if element == "bit" { (q.len() * 8, v.len() * 8) } else { (q.len(), v.len()) };
        return Err(format!(
            "The query vector has a different number of dimensions [{qd}] than the document vectors [{vd}]."
        ));
    }
    // A bit vector's L1 distance is its Hamming distance (L2: the root of
    // it); dot product and cosine aren't defined for bits.
    let bits = || -> f64 {
        q.iter()
            .zip(&v)
            .map(|(a, b)| ((*a as i8 as u8) ^ (*b as i8 as u8)).count_ones() as f64)
            .sum()
    };
    if element == "bit" {
        return match name {
            "hamming" | "l1norm" => Ok(bits()),
            "l2norm" => Ok(bits().sqrt() as f32 as f64),
            _ => Err(format!("{name} is not supported for bit vectors.")),
        };
    }
    let dot: f64 = q.iter().zip(&v).map(|(a, b)| a * b).sum();
    Ok(match name {
        "cosineSimilarity" => {
            let qm = q.iter().map(|x| x * x).sum::<f64>().sqrt();
            let vm = v.iter().map(|x| x * x).sum::<f64>().sqrt();
            dot / (qm * vm)
        }
        "dotProduct" => dot,
        "l1norm" => q.iter().zip(&v).map(|(a, b)| (a - b).abs()).sum(),
        "l2norm" => q.iter().zip(&v).map(|(a, b)| (a - b) * (a - b)).sum::<f64>().sqrt(),
        "hamming" => {
            if element == "float" {
                return Err("hamming distance is only supported for byte or bit vectors".into());
            }
            q.iter()
                .zip(&v)
                .map(|(a, b)| ((*a as i8 as u8) ^ (*b as i8 as u8)).count_ones() as f64)
                .sum()
        }
        _ => return Err(format!("Unknown call [{name}]")),
    } as f32 as f64)
}

/// The vector functions Painless scoring scripts may call.
pub const SCRIPT_FUNCTIONS: &[&str] =
    &["cosineSimilarity", "dotProduct", "l1norm", "l2norm", "hamming"];

#[cfg(test)]
mod tests {
    use super::*;

    fn field(def: Value) -> Field {
        Field::of(&def).unwrap()
    }

    #[test]
    fn scores_match_elasticsearch_transforms() {
        let q = [0.5, 1.0, -1.0];
        let v = [1.0, 2.0, 3.0];
        let l2 = field(json!({"type": "dense_vector", "similarity": "l2_norm"}));
        assert!((score(&l2, &q, &v) - 0.054_794_52).abs() < 1e-6);
        let cos = field(json!({"type": "dense_vector"}));
        assert!((score(&cos, &q, &v) - 0.455_456_46).abs() < 1e-6);
        let mip = field(json!({"type": "dense_vector", "similarity": "max_inner_product"}));
        assert!((score(&mip, &q, &v) - 0.666_666_7).abs() < 1e-6);
        let byte_dot = field(
            json!({"type": "dense_vector", "element_type": "byte", "similarity": "dot_product"}),
        );
        assert!((score(&byte_dot, &[5.0, -7.0, 9.0], &[1.0, 2.0, 3.0]) - 0.500_183_1).abs() < 1e-6);
        let bit = field(json!({"type": "dense_vector", "element_type": "bit"}));
        assert_eq!(score(&bit, &[1.0, 2.0], &[1.0, 3.0]), 0.9375);
    }

    #[test]
    fn mapping_defaults_and_errors() {
        let def = normalize("v", &json!({"type": "dense_vector", "dims": 3})).unwrap();
        assert_eq!(
            def,
            json!({"type": "dense_vector", "dims": 3, "index": true, "similarity": "cosine",
                   "index_options": {"type": "int8_hnsw", "m": 16, "ef_construction": 100}})
        );
        let bit = normalize("v", &json!({"type": "dense_vector", "element_type": "bit"})).unwrap();
        assert_eq!(bit["similarity"], "l2_norm");
        assert!(bit.get("index_options").is_none());
        assert!(normalize("v", &json!({"type": "dense_vector", "dims": 5000})).is_err());
        assert!(
            normalize(
                "v",
                &json!({"type": "dense_vector", "index": false, "similarity": "cosine"})
            )
            .is_err()
        );
        let old =
            normalize("v", &json!({"type": "dense_vector", "index_options": {"type": "hnsw"}}))
                .unwrap();
        let flat =
            normalize("v", &json!({"type": "dense_vector", "index_options": {"type": "flat"}}))
                .unwrap();
        assert_eq!(update_conflicts(&old, &flat).len(), 1);
        assert!(update_conflicts(&flat, &old).is_empty());
    }

    #[test]
    fn document_vectors_are_checked() {
        let f = field(json!({"type": "dense_vector", "dims": 3}));
        assert!(doc_vector(&f, "v", "1", &json!([1, 2, 3])).is_ok());
        assert!(doc_vector(&f, "v", "1", &json!([1, 2])).unwrap_err().1.contains("[2]"));
        assert!(
            doc_vector(&f, "v", "1", &json!([0, 0, 0])).unwrap_err().1.contains("zero magnitude")
        );
        let b = field(json!({"type": "dense_vector", "element_type": "byte", "dims": 3}));
        assert_eq!(doc_vector(&b, "b", "1", &json!("807f0a")).unwrap(), vec![-128.0, 127.0, 10.0]);
        assert!(doc_vector(&b, "b", "1", &json!([1.5, 2, 3])).is_err());
        assert!(dynamic_def(&vec![json!(0.5); 128]).is_some());
        assert!(dynamic_def(&vec![json!(1); 128]).is_none());
        assert!(dynamic_def(&vec![json!(0.5); 127]).is_none());
    }
}
