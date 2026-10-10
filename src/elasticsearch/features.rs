#![allow(clippy::neg_cmp_op_on_partial_ord, clippy::type_complexity)]
//! Feature fields: `rank_feature`, `rank_features` and `sparse_vector`,
//! each a Lucene `FeatureField` (a strictly positive float per feature
//! name, kept to 9 significant bits), with their indexing checks and the
//! queries that score them: `rank_feature` (saturation, log, sigmoid,
//! linear), `sparse_vector` with a `query_vector`, the deprecated
//! `weighted_tokens`, and `term`/`terms`/`match` on a feature field.
//!
//! Also what noida, having no machine learning or inference endpoints,
//! answers for the features needing them, as a real node without them
//! does: `text_expansion` and `sparse_vector` with an `inference_id`, and
//! `semantic_text` fields (which can't index a value) with their
//! `semantic` query.

use serde_json::{Map, Value, json};
use std::collections::HashMap;

use super::search::{CommittedDoc, EsError, raw_values};
use super::vectors::{field_def, java_float, no_inference, token_name};

type Scores = HashMap<usize, f32>;

/// The smallest positive normal float, below which Lucene refuses a
/// feature value.
const MIN_NORMAL: f32 = f32::MIN_POSITIVE;

fn type_of(def: &Value) -> Option<&str> {
    def.get("type").and_then(Value::as_str)
}

fn positive_impact(def: &Value) -> bool {
    !matches!(def.get("positive_score_impact"), Some(Value::Bool(false)))
        && def.get("positive_score_impact").and_then(Value::as_str) != Some("false")
}

/// A feature value as `FeatureField` stores it: the float's top 17 bits
/// (its term frequency), decoded.
fn stored(v: f32) -> f32 {
    f32::from_bits(encoded(v) << 15)
}

fn encoded(v: f32) -> u32 {
    v.to_bits() >> 15
}

// --- Mappings ---------------------------------------------------------

fn mapping_error(kind: &str, reason: &str) -> (u16, Value) {
    let full = format!("Failed to parse mapping: {reason}");
    (
        400,
        json!({"error": {"root_cause": [{"type": "mapper_parsing_exception", "reason": full}],
                         "type": "mapper_parsing_exception", "reason": full,
                         "caused_by": {"type": kind, "reason": reason}},
               "status": 400}),
    )
}

/// Checks the parameters of feature and `semantic_text` field
/// definitions in a mapping being put.
pub fn check_mapping(incoming: &Value) -> Result<(), (u16, Value)> {
    let Some(props) = incoming.get("properties").and_then(Value::as_object) else { return Ok(()) };
    for (name, def) in props {
        let allowed: &[&str] = match type_of(def) {
            Some("rank_feature" | "rank_features") => &["type", "positive_score_impact", "meta"],
            Some("sparse_vector") => &["type", "meta"],
            Some("semantic_text") => &["type", "inference_id", "model_settings", "meta"],
            _ => {
                check_mapping(def)?;
                continue;
            }
        };
        let ty = type_of(def).unwrap_or_default();
        if let Some(k) =
            def.as_object().and_then(|o| o.keys().find(|k| !allowed.contains(&k.as_str())))
        {
            return Err(mapping_error(
                "mapper_parsing_exception",
                &format!("unknown parameter [{k}] on mapper [{name}] of type [{ty}]"),
            ));
        }
        if let Some(v) = def.get("positive_score_impact")
            && !matches!(v, Value::Bool(_))
            && !matches!(v.as_str(), Some("true" | "false"))
        {
            let shown = v.as_str().map_or_else(|| v.to_string(), str::to_string);
            return Err(mapping_error(
                "illegal_argument_exception",
                &format!("Failed to parse value [{shown}] as only [true] or [false] are allowed."),
            ));
        }
        if ty == "semantic_text" && def.get("inference_id").is_none() {
            return Err(mapping_error(
                "illegal_argument_exception",
                "field [inference_id] must be specified",
            ));
        }
    }
    Ok(())
}

// --- Documents --------------------------------------------------------

fn doc_error(status: u16, outer: &str, kind: &str, reason: &str) -> (u16, Value) {
    let (root_kind, root_reason) =
        if status == 400 { ("document_parsing_exception", outer) } else { (kind, reason) };
    let mut err = json!({"root_cause": [{"type": root_kind, "reason": root_reason}],
                         "type": root_kind, "reason": root_reason});
    if status == 400 {
        err["caused_by"] = json!({"type": kind, "reason": reason});
    }
    (status, json!({"error": err, "status": status}))
}

/// A number, or a numeric string, as a float: `Err` with the
/// `number_format_exception` reason for another string.
fn number(v: &Value) -> Option<Result<f32, String>> {
    match v {
        Value::Number(n) => Some(Ok(n.as_f64().unwrap_or(0.0) as f32)),
        Value::String(s) => {
            Some(s.trim().parse::<f32>().map_err(|_| format!("For input string: \"{s}\"")))
        }
        _ => None,
    }
}

fn not_normal(v: f32, feature: &str, field: &str) -> String {
    format!(
        "featureValue must be a positive normal float, got: {} for feature {feature} on field \
         {field} which is less than the minimum positive normal float: 1.17549435E-38",
        java_float(v)
    )
}

fn shown(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => format!(
            "{{{}}}",
            o.iter().map(|(k, v)| format!("{k}={}", shown(v))).collect::<Vec<_>>().join(", ")
        ),
        other => other.to_string(),
    }
}

/// A `rank_feature` field's value.
fn check_rank_feature(name: &str, id: &str, v: &Value) -> Result<(), (u16, Value)> {
    let values: Vec<&Value> = match v {
        Value::Array(a) => a.iter().filter(|x| !x.is_null()).collect(),
        Value::Null => return Ok(()),
        other => vec![other],
    };
    let outer = |v: &Value| {
        format!(
            "[1:1] failed to parse field [{name}] of type [rank_feature] in document with id \
             '{id}'. Preview of field's value: '{}'",
            shown(v)
        )
    };
    if values.len() > 1 {
        return Err(doc_error(
            400,
            &outer(values[1]),
            "illegal_argument_exception",
            &format!(
                "[rank_feature] fields do not support indexing multiple values for the same \
                 field [{name}] in the same document"
            ),
        ));
    }
    let Some(v) = values.first() else { return Ok(()) };
    match number(v) {
        None => Err(doc_error(
            400,
            &outer(v),
            "x_content_parse_exception",
            &format!(
                "[1:1] Current token ({}) not numeric, can not use numeric value accessors",
                token_name(v)
            ),
        )),
        Some(Err(e)) => Err(doc_error(400, &outer(v), "number_format_exception", &e)),
        Some(Ok(x)) if !(x >= MIN_NORMAL) || !x.is_finite() => Err(doc_error(
            400,
            &outer(v),
            "illegal_argument_exception",
            &not_normal(x, name, "_feature"),
        )),
        Some(Ok(_)) => Ok(()),
    }
}

/// A `rank_features` or `sparse_vector` field's value: an object of
/// feature to positive float (several, in an array, for a
/// `sparse_vector`).
fn check_features(ty: &str, name: &str, v: &Value) -> Result<(), (u16, Value)> {
    let fail = |kind: &str, reason: String| {
        Err(doc_error(400, &format!("[1:1] failed to parse: {reason}"), kind, &reason))
    };
    fn flatten<'a>(v: &'a Value, out: &mut Vec<&'a Value>) {
        match v {
            Value::Array(a) => a.iter().for_each(|x| flatten(x, out)),
            other => out.push(other),
        }
    }
    let mut objects: Vec<&Value> = Vec::new();
    flatten(v, &mut objects);
    let mut seen: HashMap<&str, ()> = HashMap::new();
    for o in objects {
        if o.is_null() {
            continue;
        }
        let Some(features) = o.as_object() else {
            return fail(
                "illegal_argument_exception",
                format!(
                    "[{ty}] fields must be json objects, expected a START_OBJECT but got: {}",
                    token_name(o)
                ),
            );
        };
        for (feature, x) in features {
            if feature.contains('.') {
                return fail(
                    "illegal_argument_exception",
                    format!(
                        "[{ty}] fields do not support dots in feature names but found [{feature}]"
                    ),
                );
            }
            if x.is_null() {
                continue;
            }
            match number(x) {
                None => {
                    return fail(
                        "illegal_argument_exception",
                        format!(
                            "[{ty}] fields take hashes that map a feature to a strictly positive \
                             float, but got unexpected token {}",
                            token_name(x)
                        ),
                    );
                }
                Some(Err(e)) => return fail("number_format_exception", e),
                Some(Ok(f)) if !(f >= MIN_NORMAL) || !f.is_finite() => {
                    return fail("illegal_argument_exception", not_normal(f, feature, name));
                }
                Some(Ok(_)) => {}
            }
            if ty == "rank_features" && seen.insert(feature, ()).is_some() {
                return fail(
                    "illegal_argument_exception",
                    format!(
                        "[rank_features] fields do not support indexing multiple values for the \
                         same rank feature [{name}.{feature}] in the same document"
                    ),
                );
            }
        }
    }
    Ok(())
}

/// Checks a document's feature and `semantic_text` values against the
/// mapping, the way Elasticsearch checks them while indexing.
pub fn check_source(mappings: &Value, src: &Value, id: &str) -> Result<(), (u16, Value)> {
    fn walk(props: &Value, src: &Value, prefix: &str, id: &str) -> Result<(), (u16, Value)> {
        let (Some(props), Some(fields)) = (props.as_object(), src.as_object()) else {
            return Ok(());
        };
        for (k, v) in fields {
            let Some(def) = props.get(k) else { continue };
            let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            match type_of(def) {
                Some("rank_feature") => check_rank_feature(&full, id, v)?,
                Some(ty @ ("rank_features" | "sparse_vector")) => check_features(ty, &full, v)?,
                Some("semantic_text") if !v.is_null() => {
                    let inference = def.get("inference_id").and_then(Value::as_str).unwrap_or("");
                    return Err(doc_error(
                        404,
                        "",
                        "resource_not_found_exception",
                        &format!("Inference id [{inference}] not found for field [{full}]"),
                    ));
                }
                _ => {
                    if let Some(p) = def.get("properties") {
                        match v {
                            Value::Array(a) => {
                                for e in a {
                                    walk(p, e, &full, id)?;
                                }
                            }
                            _ => walk(p, v, &full, id)?,
                        }
                    }
                }
            }
        }
        Ok(())
    }
    match mappings.get("properties") {
        Some(props) => walk(props, src, "", id),
        None => Ok(()),
    }
}

// --- Queries ----------------------------------------------------------

fn shard_error(reason: &str) -> EsError {
    EsError::shard_failure("query_shard_exception", &format!("failed to create query: {reason}"))
        .caused_by("illegal_argument_exception", reason)
}

/// A feature field's stored values in a document: feature to value (the
/// largest, for a feature given more than once).
fn doc_features(def: &Value, d: &CommittedDoc, field: &str) -> HashMap<String, f32> {
    let invert = !positive_impact(def);
    let mut out: HashMap<String, f32> = HashMap::new();
    for o in raw_values(&d.source, field) {
        let Some(o) = o.as_object() else { continue };
        for (k, v) in o {
            if let Some(Ok(x)) = number(v) {
                let x = stored(if invert { 1.0 / x } else { x });
                let e = out.entry(k.clone()).or_insert(x);
                *e = e.max(x);
            }
        }
    }
    out
}

/// A `rank_feature` field's stored value in a document.
fn doc_feature(def: &Value, d: &CommittedDoc, field: &str) -> Option<f32> {
    let v = raw_values(&d.source, field).into_iter().find(|v| !v.is_null())?;
    let x = number(v)?.ok()?;
    Some(stored(if positive_impact(def) { x } else { 1.0 / x }))
}

/// Each document's stored value of one feature: a `rank_feature` field,
/// or `field.feature` of a `rank_features` field.
fn feature_values(
    mappings: &Value,
    docs: &[CommittedDoc],
    field: &str,
) -> Result<Option<(bool, Vec<(usize, f32)>)>, EsError> {
    if let Some(def) = field_def(mappings, field).filter(|d| type_of(d).is_some()) {
        return match type_of(def) {
            Some("rank_feature") => Ok(Some((
                positive_impact(def),
                docs.iter()
                    .enumerate()
                    .filter_map(|(i, d)| doc_feature(def, d, field).map(|x| (i, x)))
                    .collect(),
            ))),
            other => Err(shard_error(&format!(
                "[rank_feature] query only works on [rank_feature] fields and features of \
                 [rank_features] fields, not [{}]",
                other.unwrap_or("object")
            ))),
        };
    }
    let Some((parent, feature)) = field.rsplit_once('.') else { return Ok(None) };
    let Some(def) = field_def(mappings, parent).filter(|d| type_of(d) == Some("rank_features"))
    else {
        return Ok(None);
    };
    let values = docs
        .iter()
        .enumerate()
        .filter_map(|(i, d)| doc_features(def, d, parent).get(feature).map(|x| (i, *x)))
        .collect();
    Ok(Some((positive_impact(def), values)))
}

fn parse_error(reason: &str) -> EsError {
    EsError::new(400, "x_content_parse_exception", reason)
}

/// Lucene's default saturation pivot: the stored value of the mean of
/// the feature's encoded values (roughly their geometric mean).
fn default_pivot(values: &[(usize, f32)]) -> f32 {
    if values.is_empty() {
        return 1.0;
    }
    let total: u64 = values.iter().map(|(_, x)| encoded(*x) as u64).sum();
    let mean = (total as f64 / values.len() as f64) as f32;
    f32::from_bits((mean as u32) << 15)
}

fn param(o: &Map<String, Value>, key: &str) -> Option<f32> {
    o.get(key)
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .map(|f| f as f32)
}

/// The `rank_feature` query.
fn rank_feature(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let Some(o) = v.as_object() else {
        return Err(EsError::parsing(
            "[rank_feature] query malformed, no start_object after query name",
        ));
    };
    const KEYS: &[&str] = &["field", "boost", "_name", "saturation", "log", "sigmoid", "linear"];
    if let Some(k) = o.keys().find(|k| !KEYS.contains(&k.as_str())) {
        return Err(parse_error(&format!("[feature] unknown field [{k}]")));
    }
    let function_spec =
        |name: &str, required: &[&str]| -> Result<Option<Map<String, Value>>, EsError> {
            let Some(spec) = o.get(name) else { return Ok(None) };
            let spec = spec.as_object().cloned().unwrap_or_default();
            if let Some(missing) = required.iter().find(|k| !spec.contains_key(**k)) {
                return Err(parse_error(&format!("[feature] failed to parse field [{name}]"))
                    .caused_by("illegal_argument_exception", &format!("Required [{missing}]")));
            }
            Ok(Some(spec))
        };
    let saturation = function_spec("saturation", &[])?;
    let log = function_spec("log", &["scaling_factor"])?;
    let sigmoid = function_spec("sigmoid", &["pivot", "exponent"])?;
    let linear = function_spec("linear", &[])?;
    let Some(field) = o.get("field").and_then(Value::as_str) else {
        return Err(EsError::new(400, "illegal_argument_exception", "Required [field]"));
    };
    let given = [&saturation, &log, &sigmoid, &linear].iter().filter(|f| f.is_some()).count();
    if given > 1 {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            "Failed to build [feature] after last required field arrived",
        )
        .caused_by(
            "illegal_argument_exception",
            "Can only specify one of [log], [saturation], [sigmoid] and [linear]",
        ));
    }
    let boost = param(o, "boost").unwrap_or(1.0);
    let Some((positive, values)) = feature_values(mappings, docs, field)? else {
        return Ok(Scores::new());
    };
    let score: Box<dyn Fn(f32) -> f32> = if let Some(l) = log {
        if !positive {
            return Err(shard_error(
                "Cannot use the [log] function with a field that has a negative score impact as \
                 it would trigger negative scores",
            ));
        }
        let factor = param(&l, "scaling_factor").unwrap_or(1.0);
        if !(factor >= 1.0) {
            return Err(shard_error(&format!(
                "scalingFactor must be >= 1, got: {}",
                java_float(factor)
            )));
        }
        Box::new(move |f| (boost as f64 * ((factor + f) as f64).ln()) as f32)
    } else if let Some(s) = sigmoid {
        let pivot = param(&s, "pivot").unwrap_or(1.0);
        let exp = param(&s, "exponent").unwrap_or(1.0);
        if !(pivot > 0.0) {
            return Err(shard_error(&format!("pivot must be > 0, got: {}", java_float(pivot))));
        }
        if !(exp > 0.0) {
            return Err(shard_error(&format!("a must be > 0, got: {}", java_float(exp))));
        }
        let pivot_pa = (pivot as f64).powf(exp as f64);
        Box::new(move |f| {
            (boost as f64 * (1.0 - pivot_pa / ((f as f64).powf(exp as f64) + pivot_pa))) as f32
        })
    } else if linear.is_some() {
        Box::new(move |f| boost * f)
    } else {
        let pivot = match saturation.as_ref().and_then(|s| param(s, "pivot")) {
            Some(p) => p,
            None => default_pivot(&values),
        };
        if !(pivot > 0.0) {
            return Err(shard_error(&format!("pivot must be > 0, got: {}", java_float(pivot))));
        }
        Box::new(move |f| boost * (1.0 - pivot / (f + pivot)))
    };
    Ok(values.into_iter().map(|(i, f)| (i, score(f))).collect())
}

/// Each document's linear score for weighted tokens on a feature field:
/// the sum of weight times stored value over the tokens it has.
fn weighted(def: &Value, field: &str, tokens: &[(String, f32)], docs: &[CommittedDoc]) -> Scores {
    docs.iter()
        .enumerate()
        .filter_map(|(i, d)| {
            let features = doc_features(def, d, field);
            // Lucene sums a disjunction's clause scores in double.
            let mut total = 0f64;
            let mut any = false;
            for (t, w) in tokens {
                if let Some(x) = features.get(t) {
                    total += (w * x) as f64;
                    any = true;
                }
            }
            any.then_some((i, total as f32))
        })
        .collect()
}

/// `tokens_freq_ratio_threshold`, `tokens_weight_threshold`,
/// `only_score_pruned_tokens`.
struct Pruning {
    ratio: f32,
    weight: f32,
    only_pruned: bool,
}

/// `pruning_config` (on with `prune`, or given): why not, as (type,
/// reason).
fn pruning_config(v: Option<&Value>, prune: bool) -> Result<Option<Pruning>, (String, String)> {
    let bad = |reason: &str| ("illegal_argument_exception".to_string(), reason.to_string());
    let spec = match v {
        Some(Value::Object(o)) => Some(o.clone()),
        Some(other) => {
            return Err((
                "parsing_exception".into(),
                format!("[pruning_config] unknown token [{}]", token_name(other)),
            ));
        }
        None => None,
    };
    if let Some(o) = &spec
        && let Some(k) = o.keys().find(|k| {
            !matches!(
                k.as_str(),
                "tokens_freq_ratio_threshold"
                    | "tokens_weight_threshold"
                    | "only_score_pruned_tokens"
            )
        })
    {
        return Err(("parsing_exception".into(), format!("[pruning_config] unknown token [{k}]")));
    }
    let o = spec.clone().unwrap_or_default();
    let ratio = param(&o, "tokens_freq_ratio_threshold").unwrap_or(5.0);
    if !(1.0..=100.0).contains(&ratio) {
        return Err(bad(&format!(
            "[tokens_freq_ratio_threshold] must be between [1] and [100], got {}",
            java_float(ratio)
        )));
    }
    let weight = param(&o, "tokens_weight_threshold").unwrap_or(0.4);
    if !(0.0..=1.0).contains(&weight) {
        return Err(bad("[tokens_weight_threshold] must be between 0 and 1"));
    }
    let only_pruned = o.get("only_score_pruned_tokens").and_then(Value::as_bool).unwrap_or(false);
    Ok((prune || spec.is_some()).then_some(Pruning { ratio, weight, only_pruned }))
}

/// Drops (or, with `only_score_pruned_tokens`, keeps only) the tokens
/// that are frequent in the field yet weigh little against the heaviest:
/// Elasticsearch's token pruning. A token no document has is dropped
/// either way. (Elasticsearch averages the frequency ratio over every
/// indexed term of the index; noida over the field's own features.)
fn prune(
    def: &Value,
    field: &str,
    tokens: Vec<(String, f32)>,
    config: &Pruning,
    docs: &[CommittedDoc],
) -> Vec<(String, f32)> {
    let mut freq: HashMap<String, usize> = HashMap::new();
    let mut with_field = 0usize;
    for d in docs {
        let f = doc_features(def, d, field);
        if !f.is_empty() {
            with_field += 1;
        }
        for k in f.keys() {
            *freq.entry(k.clone()).or_default() += 1;
        }
    }
    if with_field == 0 || freq.is_empty() {
        return Vec::new();
    }
    let average: f32 =
        freq.values().map(|n| *n as f32 / with_field as f32).sum::<f32>() / freq.len() as f32;
    let best = tokens.iter().map(|t| t.1).fold(0f32, f32::max);
    tokens
        .into_iter()
        .filter(|(t, w)| {
            let keep = freq.get(t).is_some_and(|n| {
                let ratio = *n as f32 / with_field as f32;
                ratio < config.ratio * average || *w > config.weight * best
            });
            keep != config.only_pruned
        })
        .collect()
}

/// `{"token": weight, ...}` (or an array of such objects).
fn parse_tokens(v: Option<&Value>, owner: &str) -> Result<Vec<(String, f32)>, EsError> {
    let weighted = owner == "weighted_tokens";
    let objects: Vec<&Value> = match v {
        Some(Value::Array(a)) => a.iter().collect(),
        Some(o @ Value::Object(_)) => vec![o],
        _ => Vec::new(),
    };
    let mut out = Vec::new();
    for o in objects {
        for (k, w) in o.as_object().into_iter().flatten() {
            match number(w) {
                Some(Ok(x)) => out.push((k.clone(), x)),
                Some(Err(e)) if weighted => {
                    return Err(EsError::new(400, "number_format_exception", &e));
                }
                _ => {
                    return Err(EsError::parsing(&format!(
                        "Failed to build [{owner}] after last required field arrived"
                    ))
                    .caused_by(
                        "illegal_argument_exception",
                        &format!("weight must be a number, was [{}]", shown(w)),
                    ));
                }
            }
        }
    }
    Ok(out)
}

/// A feature field's weighted tokens, scored (after any pruning); token
/// weights must be positive, as `FeatureField` boosts are.
fn score_tokens(
    def: &Value,
    field: &str,
    tokens: Vec<(String, f32)>,
    pruning: Option<Pruning>,
    boost: f32,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    if let Some((_, w)) = tokens.iter().find(|(_, w)| !(*w > 0.0) || !w.is_finite()) {
        return Err(shard_error(&format!(
            "boost must be a positive float, got {}",
            java_float(*w)
        )));
    }
    let tokens = match &pruning {
        Some(p) => prune(def, field, tokens, p, docs),
        None => tokens,
    };
    let mut scores = weighted(def, field, &tokens, docs);
    scores.values_mut().for_each(|s| *s *= boost);
    Ok(scores)
}

/// The `sparse_vector` query.
fn sparse_vector(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let o = v.as_object().cloned().unwrap_or_default();
    const KEYS: &[&str] = &[
        "field",
        "query_vector",
        "inference_id",
        "query",
        "prune",
        "pruning_config",
        "boost",
        "_name",
    ];
    if let Some(k) = o.keys().find(|k| !KEYS.contains(&k.as_str())) {
        return Err(EsError::parsing(&format!("[sparse_vector] unknown field [{k}]")).caused_by(
            "x_content_parse_exception",
            &format!("[sparse_vector] unknown field [{k}]"),
        ));
    }
    let Some(field) = o.get("field").and_then(Value::as_str) else {
        return Err(EsError::parsing("Required [field]")
            .caused_by("illegal_argument_exception", "Required [field]"));
    };
    let build_failure = |reason: &str| {
        EsError::parsing("Failed to build [sparse_vector] after last required field arrived")
            .caused_by("illegal_argument_exception", reason)
    };
    let pruning = pruning_config(
        o.get("pruning_config"),
        o.get("prune").and_then(Value::as_bool).unwrap_or(false),
    )
    .map_err(|(kind, reason)| {
        let outer = "[sparse_vector] failed to parse field [pruning_config]";
        EsError::parsing(outer)
            .caused_by("x_content_parse_exception", &format!("{outer}: {kind}: {reason}"))
    })?;
    let tokens = match (o.get("query_vector"), o.get("inference_id")) {
        (Some(_), Some(_)) | (None, None) => {
            return Err(build_failure(
                "[sparse_vector] requires one of [query_vector] or [inference_id]",
            ));
        }
        (None, Some(_)) if o.get("query").is_none() => {
            return Err(build_failure(
                "[sparse_vector] requires [query] when [inference_id] is specified",
            ));
        }
        (None, Some(_)) => return Err(no_inference()),
        (Some(q), None) => parse_tokens(Some(q), "sparse_vector")?,
    };
    let Some(def) = field_def(mappings, field) else { return Ok(Scores::new()) };
    let ty = type_of(def).unwrap_or("object");
    if ty != "sparse_vector" {
        return Err(shard_error(&format!(
            "field [{field}] must be type [sparse_vector] but is type [{ty}]"
        )));
    }
    let boost = param(&o, "boost").unwrap_or(1.0);
    score_tokens(def, field, tokens, pruning, boost, docs)
}

/// The deprecated `weighted_tokens` query: `{"<field>": {"tokens": ...}}`.
fn weighted_tokens(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let Some((field, spec)) = v.as_object().and_then(|o| o.iter().next()) else {
        return Err(EsError::parsing("No fieldname specified for query"));
    };
    let o = spec.as_object().cloned().unwrap_or_default();
    if let Some(k) =
        o.keys().find(|k| !matches!(k.as_str(), "tokens" | "pruning_config" | "boost" | "_name"))
    {
        return Err(EsError::parsing(&format!("unknown field [{k}]")));
    }
    let tokens = parse_tokens(o.get("tokens"), "weighted_tokens")?;
    if tokens.is_empty() {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            "[weighted_tokens] requires at least one token",
        ));
    }
    let pruning = pruning_config(o.get("pruning_config"), false)
        .map_err(|(kind, reason)| EsError::new(400, &kind, &reason))?;
    let Some(def) = field_def(mappings, field) else { return Ok(Scores::new()) };
    let ty = type_of(def).unwrap_or("object");
    if !matches!(ty, "sparse_vector" | "rank_features") {
        let reason = format!(
            "[{ty}] is not an appropriate field type for this query. Allowed field types are \
             [rank_features, sparse_vector]."
        );
        return Err(EsError::shard_failure(
            "query_shard_exception",
            &format!("failed to create query: {reason}"),
        )
        .caused_by("parse_exception", &reason));
    }
    let boost = param(&o, "boost").unwrap_or(1.0);
    score_tokens(def, field, tokens, pruning, boost, docs)
}

/// The deprecated `text_expansion` query: needs a model.
fn text_expansion(v: &Value) -> EsError {
    let spec = v.as_object().and_then(|o| o.values().next()).cloned().unwrap_or_default();
    if spec.get("model_id").is_none() {
        return EsError::new(
            400,
            "illegal_argument_exception",
            "[text_expansion] requires a model_id value",
        );
    }
    if spec.get("model_text").is_none() {
        return EsError::parsing("No text specified for text query");
    }
    no_inference()
}

/// The `semantic` query: needs the field's inference endpoint.
fn semantic(v: &Value, mappings: &Value) -> Result<Scores, EsError> {
    let o = v.as_object().cloned().unwrap_or_default();
    if let Some(k) = o.keys().find(|k| !matches!(k.as_str(), "field" | "query" | "boost" | "_name"))
    {
        return Err(parse_error(&format!("[semantic] unknown field [{k}]")));
    }
    for required in ["field", "query"] {
        if o.get(required).is_none() {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                &format!("Required [{required}]"),
            ));
        }
    }
    let field = o.get("field").and_then(Value::as_str).unwrap_or("");
    let Some(def) = field_def(mappings, field) else { return Ok(Scores::new()) };
    match type_of(def) {
        Some("semantic_text") => {
            let id = def.get("inference_id").and_then(Value::as_str).unwrap_or("");
            Err(EsError::new(
                404,
                "resource_not_found_exception",
                &format!("Inference endpoint not found [{id}]"),
            ))
        }
        other => Err(EsError::shard_failure(
            "illegal_argument_exception",
            &format!(
                "Field [{field}] of type [{}] does not support semantic queries",
                other.unwrap_or("object")
            ),
        )),
    }
}

/// The single field a leaf query (`term`, `match`, ...) names, with its
/// spec.
fn leaf_field(spec: &Value) -> Option<(&str, &Value)> {
    let o = spec.as_object()?;
    o.iter().find(|(k, _)| !matches!(k.as_str(), "boost" | "_name")).map(|(k, v)| (k.as_str(), v))
}

/// A standard query on a feature or `semantic_text` field: how
/// Elasticsearch answers it.
fn on_feature_field(
    kind: &str,
    spec: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Option<Result<Scores, EsError>> {
    if kind == "exists" {
        let field = spec.get("field")?.as_str()?;
        let def = field_def(mappings, field)?;
        return (type_of(def) == Some("rank_features"))
            .then(|| Err(shard_error("[rank_features] fields do not support [exists] queries")));
    }
    let (field, v) = leaf_field(spec)?;
    let def = field_def(mappings, field)?;
    let ty = type_of(def)?;
    if !matches!(ty, "rank_feature" | "rank_features" | "sparse_vector" | "semantic_text") {
        return None;
    }
    let unsupported = |what: &str| {
        Some(Err(shard_error(&format!(
            "Field [{field}] of type [{ty}] does not support {what} queries"
        ))))
    };
    match kind {
        "prefix" | "wildcard" => {
            return Some(Err(EsError::shard_failure(
                "query_shard_exception",
                &format!(
                    "Can only use {kind} queries on keyword, text and wildcard fields - not on \
                     [{field}] which is of type [{ty}]"
                ),
            )));
        }
        "regexp" => {
            return Some(Err(EsError::shard_failure(
                "query_shard_exception",
                &format!(
                    "Can only use regexp queries on keyword and text fields - not on [{field}] \
                     which is of type [{ty}]"
                ),
            )));
        }
        "fuzzy" => {
            return Some(Err(shard_error(&format!(
                "Can only use fuzzy queries on keyword and text fields - not on [{field}] which \
                 is of type [{ty}]"
            ))));
        }
        "range" => return unsupported("range"),
        _ => {}
    }
    if ty == "semantic_text" {
        return (kind == "match").then(|| unsupported("match")).flatten();
    }
    if ty == "rank_feature" {
        return match kind {
            "match" | "match_phrase" => unsupported("match"),
            "term" | "terms" => {
                Some(Err(shard_error("Queries on [rank_feature] fields are not supported")))
            }
            _ => None,
        };
    }
    let boost = |o: &Value| o.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    match kind {
        // A term on a feature field scores the feature's value.
        "term" | "match" | "match_phrase" => {
            let (value, b) = match v {
                Value::Object(o) => {
                    let value =
                        o.get("value").or_else(|| o.get("query")).cloned().unwrap_or_default();
                    (value, boost(v))
                }
                other => (other.clone(), 1.0),
            };
            let token = value.as_str().map_or_else(|| value.to_string(), str::to_string);
            Some(Ok(weighted(def, field, &[(token, b)], docs)))
        }
        // A terms query scores constant.
        "terms" => {
            let wanted: Vec<String> = v
                .as_array()?
                .iter()
                .map(|t| t.as_str().map_or_else(|| t.to_string(), str::to_string))
                .collect();
            let b = boost(spec);
            Some(Ok(docs
                .iter()
                .enumerate()
                .filter(|(_, d)| doc_features(def, d, field).keys().any(|k| wanted.contains(k)))
                .map(|(i, _)| (i, b))
                .collect()))
        }
        _ => None,
    }
}

/// The queries this module answers (`rank_feature`, `sparse_vector`,
/// ...), and standard queries on feature fields; `None` for any other.
pub fn eval(
    query: &Map<String, Value>,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Option<Result<Scores, EsError>> {
    let (kind, spec) = query.iter().next()?;
    Some(match kind.as_str() {
        "rank_feature" => rank_feature(spec, mappings, docs),
        "sparse_vector" => sparse_vector(spec, mappings, docs),
        "weighted_tokens" => weighted_tokens(spec, mappings, docs),
        "text_expansion" => Err(text_expansion(spec)),
        "semantic" => semantic(spec, mappings),
        _ => return on_feature_field(kind, spec, mappings, docs),
    })
}

/// The deprecation warnings a search body's queries earn.
pub fn warnings(body: &Value) -> Vec<&'static str> {
    fn walk(v: &Value, out: &mut Vec<&'static str>) {
        match v {
            Value::Object(o) => {
                for (k, x) in o {
                    let w = match k.as_str() {
                        "weighted_tokens" => Some(
                            "weighted_tokens is deprecated and will be removed. Use sparse_vector instead.",
                        ),
                        "text_expansion" => {
                            Some("text_expansion is deprecated. Use sparse_vector instead.")
                        }
                        _ => None,
                    };
                    if let Some(w) = w
                        && !out.contains(&w)
                    {
                        out.push(w);
                    }
                    walk(x, out);
                }
            }
            Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    for key in ["query", "knn", "retriever", "sub_searches", "post_filter"] {
        if let Some(v) = body.get(key) {
            walk(v, &mut out);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: &str, source: Value) -> CommittedDoc {
        CommittedDoc {
            index: "i".into(),
            id: id.into(),
            source,
            version: 1,
            seq: 0,
            full_source: None,
            tsid: None,
        }
    }

    fn mappings() -> Value {
        json!({"properties": {
            "t": {"type": "sparse_vector"},
            "rf": {"type": "rank_feature"},
            "rfs": {"type": "rank_features"},
            "neg": {"type": "rank_feature", "positive_score_impact": false},
        }})
    }

    fn docs() -> Vec<CommittedDoc> {
        vec![
            doc(
                "1",
                json!({"t": {"a": 1.5, "b": 0.33333}, "rf": 10, "rfs": {"x": 2.5, "y": 7}, "neg": 3}),
            ),
            doc(
                "2",
                json!({"t": [{"a": 0.2, "c": 3.1}, {"a": 2.7}], "rf": 0.5, "rfs": {"x": 11}, "neg": 0.25}),
            ),
            doc("3", json!({"t": {"d": 1}, "rf": 100})),
        ]
    }

    fn run(q: Value) -> Result<Scores, EsError> {
        let m = mappings();
        eval(q.as_object().unwrap(), &m, &docs()).unwrap()
    }

    // Scores as Elasticsearch 8.15 gives them for these documents.
    #[test]
    fn rank_feature_functions() {
        let s = run(json!({"rank_feature": {"field": "rf"}})).unwrap();
        assert_eq!((s[&2], s[&0], s[&1]), (0.928_074_24, 0.563_380_24, 0.060_606_062));
        let s =
            run(json!({"rank_feature": {"field": "rfs.x", "log": {"scaling_factor": 2}}})).unwrap();
        assert_eq!((s[&1], s[&0]), (2.564_949_3, 1.504_077_4));
        let s = run(json!({"rank_feature": {"field": "neg"}})).unwrap();
        assert_eq!((s[&1], s[&0]), (0.774_583_94, 0.222_439_65));
        let s =
            run(json!({"rank_feature": {"field": "rf", "sigmoid": {"pivot": 5, "exponent": 0.6}}}))
                .unwrap();
        assert_eq!(s[&2], 0.857_836_96);
        let s = run(json!({"rank_feature": {"field": "rf", "saturation": {"pivot": 5}}})).unwrap();
        assert_eq!(s[&1], 0.090_909_064);
    }

    #[test]
    fn sparse_vector_scores_stored_values() {
        let s = run(
            json!({"sparse_vector": {"field": "t", "query_vector": {"a": 1, "b": 2, "c": 0.5}}}),
        )
        .unwrap();
        assert_eq!((s[&1], s[&0]), (4.242_187_5, 2.166_015_6));
        let s = run(json!({"term": {"t": "a"}})).unwrap();
        assert_eq!((s[&1], s[&0]), (2.695_312_5, 1.5));
        let e = run(json!({"sparse_vector": {"field": "t", "inference_id": "m", "query": "x"}}))
            .unwrap_err();
        assert_eq!(e.status, 500);
    }

    #[test]
    fn indexing_checks() {
        let m = mappings();
        assert!(check_source(&m, &json!({"rf": -1}), "x").is_err());
        assert!(check_source(&m, &json!({"rf": "5"}), "x").is_ok());
        assert!(check_source(&m, &json!({"rfs": {"x.y": 1}}), "x").is_err());
        assert!(check_source(&m, &json!({"rfs": [{"x": 1}, {"x": 2}]}), "x").is_err());
        assert!(check_source(&m, &json!({"t": [{"a": 1}, {"a": 2}]}), "x").is_ok());
        assert!(check_source(&m, &json!({"t": {"a": 0}}), "x").is_err());
        let sem = json!({"properties": {"s": {"type": "semantic_text", "inference_id": "e"}}});
        assert_eq!(check_source(&sem, &json!({"s": "hi"}), "x").unwrap_err().0, 404);
        assert!(check_mapping(&json!({"properties": {"s": {"type": "semantic_text"}}})).is_err());
    }
}
