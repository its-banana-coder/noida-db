//! Query types beyond the core set in `search.rs`: autocomplete
//! (`match_phrase_prefix`, `match_bool_prefix`), relevance tuning
//! (`boosting`, `function_score`, `script_score`), `combined_fields`, and
//! geo (`geo_distance`, `geo_bounding_box`). Scores follow
//! Elasticsearch's formulas so ordering and `_score` match.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use super::search::EsError;
use super::search::{
    CommittedDoc, analyze_for, bm25_scores, doc_tokens, eval, field_and_spec, query_text,
    raw_values, resolve_field, sloppy_phrase_matches,
};
use super::{dates, painless};

type Scores = HashMap<usize, f32>;

fn obj_spec(spec: &Value) -> (String, Option<&Map<String, Value>>) {
    match spec.as_object() {
        Some(o) => (query_text(o.get("query")), Some(o)),
        None => (query_text(Some(spec)), None),
    }
}

fn boost_of(o: Option<&Map<String, Value>>) -> f32 {
    o.and_then(|o| o.get("boost")).and_then(Value::as_f64).unwrap_or(1.0) as f32
}

/// The terms of `field` starting with `prefix`, in term order, at most
/// `max` of them (Lucene's MultiPhraseQuery expansion).
fn expansions(per_doc: &[Vec<String>], prefix: &str, max: usize) -> Vec<String> {
    let mut all: Vec<&String> =
        per_doc.iter().flatten().filter(|t| t.starts_with(prefix)).collect();
    all.sort();
    all.dedup();
    all.into_iter().take(max).cloned().collect()
}

/// `match_phrase_prefix`: a phrase whose last term is a prefix.
pub fn match_phrase_prefix(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Scores {
    let Some((field, spec)) = field_and_spec(v) else { return Scores::new() };
    let (text, o) = obj_spec(spec);
    let slop = o.and_then(|o| o.get("slop")).and_then(Value::as_u64).unwrap_or(0) as usize;
    let max =
        o.and_then(|o| o.get("max_expansions")).and_then(Value::as_u64).unwrap_or(50) as usize;
    let terms = analyze_for(mappings, field, &text);
    let Some((last, head)) = terms.split_last() else { return Scores::new() };
    let per_doc = doc_tokens(mappings, docs, field);
    let mut out = Scores::new();
    for exp in expansions(&per_doc, last, max) {
        let mut phrase = head.to_vec();
        phrase.push(exp);
        let mut sc = bm25_scores(mappings, docs, field, &phrase, true);
        sc.retain(|i, _| sloppy_phrase_matches(&per_doc[*i], &phrase, slop));
        for (i, s) in sc {
            let e = out.entry(i).or_insert(0.0);
            *e = e.max(s);
        }
    }
    let b = boost_of(o);
    out.values_mut().for_each(|s| *s *= b);
    out
}

/// `match_bool_prefix`: every term a should/must `term` clause, the last
/// one a constant-score `prefix`.
pub fn match_bool_prefix(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Scores {
    let Some((field, spec)) = field_and_spec(v) else { return Scores::new() };
    let (text, o) = obj_spec(spec);
    let and = o
        .and_then(|o| o.get("operator"))
        .and_then(Value::as_str)
        .is_some_and(|op| op.eq_ignore_ascii_case("and"));
    let terms = analyze_for(mappings, field, &text);
    let Some((last, head)) = terms.split_last() else { return Scores::new() };
    let per_doc = doc_tokens(mappings, docs, field);
    let mut clauses: Vec<Scores> = head
        .iter()
        .map(|t| bm25_scores(mappings, docs, field, std::slice::from_ref(t), false))
        .collect();
    clauses.push(
        per_doc
            .iter()
            .enumerate()
            .filter(|(_, toks)| toks.iter().any(|t| t.starts_with(last.as_str())))
            .map(|(i, _)| (i, 1.0))
            .collect(),
    );
    let mut out = Scores::new();
    let mut hits: HashMap<usize, usize> = HashMap::new();
    for c in &clauses {
        for (i, s) in c {
            *out.entry(*i).or_insert(0.0) += s;
            *hits.entry(*i).or_insert(0) += 1;
        }
    }
    if and {
        out.retain(|i, _| hits.get(i) == Some(&clauses.len()));
    }
    let b = boost_of(o);
    out.values_mut().for_each(|s| *s *= b);
    out
}

/// `boosting`: positive matches, demoted by `negative_boost` when the
/// negative query matches too.
pub fn boosting(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let pos = v
        .get("positive")
        .ok_or_else(|| EsError::parsing("[boosting] query requires 'positive' query to be set'"))?;
    let neg = v
        .get("negative")
        .ok_or_else(|| EsError::parsing("[boosting] query requires 'negative' query to be set'"))?;
    let nb = v.get("negative_boost").and_then(Value::as_f64).ok_or_else(|| {
        EsError::parsing(
            "[boosting] query requires 'negative_boost' to be set to be a positive value'",
        )
    })? as f32;
    let negative = eval(neg, mappings, docs)?;
    let mut out = eval(pos, mappings, docs)?;
    for (i, s) in out.iter_mut() {
        if negative.contains_key(i) {
            *s *= nb;
        }
    }
    let b = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    out.values_mut().for_each(|s| *s *= b);
    Ok(out)
}

/// A document's numeric values of `field` (dates as epoch millis).
fn numbers(mappings: &Value, d: &CommittedDoc, field: &str) -> Vec<f64> {
    let is_date =
        matches!(resolve_field(mappings, field).1.as_deref(), Some("date" | "date_nanos"));
    raw_values(&d.source, field)
        .into_iter()
        .filter_map(|v| {
            if is_date {
                dates::value_millis(v, None).map(|m| m as f64)
            } else {
                v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            }
        })
        .collect()
}

/// A decay function's `origin`/`scale`/`offset` value: a number, a date
/// (math), or a duration (`30d`, `2h`) as millis.
fn decay_param(v: Option<&Value>, date: bool, duration: bool) -> Option<f64> {
    let v = v?;
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    let s = v.as_str()?;
    if date && duration {
        return duration_millis(s);
    }
    if date {
        return dates::parse_math(s, dates::now_ms(), false, None, 0).map(|m| m as f64);
    }
    s.parse().ok()
}

fn duration_millis(s: &str) -> Option<f64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let (n, unit) = s.split_at(split);
    let n: f64 = n.parse().ok()?;
    let ms = match unit {
        "ms" => 1.0,
        "s" => 1e3,
        "m" => 60e3,
        "h" => 3600e3,
        "d" => 86400e3,
        "w" => 7.0 * 86400e3,
        _ => return None,
    };
    Some(n * ms)
}

#[derive(Clone, Copy)]
enum Decay {
    Gauss,
    Exp,
    Linear,
}

/// One `function_score` function's value for a document (None: the
/// function doesn't apply).
fn function_value(
    f: &Map<String, Value>,
    mappings: &Value,
    d: &CommittedDoc,
    score: f32,
) -> Result<Option<f64>, EsError> {
    let weight = f.get("weight").and_then(Value::as_f64);
    let mut value: Option<f64> = None;
    if let Some(fvf) = f.get("field_value_factor") {
        let field = fvf.get("field").and_then(Value::as_str).unwrap_or("");
        let factor = fvf.get("factor").and_then(Value::as_f64).unwrap_or(1.0);
        let modifier = fvf.get("modifier").and_then(Value::as_str).unwrap_or("none");
        let raw = match numbers(mappings, d, field).first() {
            Some(x) => *x,
            None => match fvf.get("missing").and_then(Value::as_f64) {
                Some(m) => m,
                None => {
                    return Err(EsError::new(
                        400,
                        "illegal_argument_exception",
                        &format!("Missing value for field [{field}]"),
                    ));
                }
            },
        };
        let x = factor * raw;
        value = Some(match modifier {
            "log" => x.log10(),
            "log1p" => (x + 1.0).log10(),
            "log2p" => (x + 2.0).log10(),
            "ln" => x.ln(),
            "ln1p" => (x + 1.0).ln(),
            "ln2p" => (x + 2.0).ln(),
            "square" => x * x,
            "sqrt" => x.sqrt(),
            "reciprocal" => 1.0 / x,
            _ => x,
        });
    }
    for (key, kind) in [("gauss", Decay::Gauss), ("exp", Decay::Exp), ("linear", Decay::Linear)] {
        let Some(spec) = f.get(key).and_then(Value::as_object) else { continue };
        let Some((field, p)) = spec.iter().find(|(k, _)| !matches!(k.as_str(), "multi_value_mode"))
        else {
            continue;
        };
        let is_date =
            matches!(resolve_field(mappings, field).1.as_deref(), Some("date" | "date_nanos"));
        let origin = match p.get("origin") {
            Some(o) => decay_param(Some(o), is_date, false),
            None if is_date => Some(dates::now_ms() as f64),
            None => None,
        }
        .ok_or_else(|| EsError::parsing(&format!("[{key}] must supply an origin")))?;
        let scale = decay_param(p.get("scale"), is_date, true)
            .ok_or_else(|| EsError::parsing(&format!("[{key}] must supply a scale")))?;
        let offset = decay_param(p.get("offset"), is_date, true).unwrap_or(0.0);
        let decay = p.get("decay").and_then(Value::as_f64).unwrap_or(0.5);
        let vals = numbers(mappings, d, field);
        // A document without the field isn't decayed.
        let v = match vals
            .iter()
            .map(|v| (v - origin).abs())
            .fold(None, |m: Option<f64>, x| Some(m.map_or(x, |m| m.min(x))))
        {
            None => 1.0,
            Some(dist) => {
                let dist = (dist - offset).max(0.0);
                match kind {
                    Decay::Gauss => {
                        let sigma2 = -scale * scale / (2.0 * decay.ln());
                        (-dist * dist / (2.0 * sigma2)).exp()
                    }
                    Decay::Exp => (decay.ln() / scale * dist).exp(),
                    Decay::Linear => {
                        let s = scale / (1.0 - decay);
                        ((s - dist) / s).max(0.0)
                    }
                }
            }
        };
        value = Some(v);
    }
    if let Some(script) = f.get("script_score").and_then(|s| s.get("script")) {
        value = Some(run_score_script(script, mappings, d, score)?);
    }
    if f.contains_key("random_score") {
        // Deterministic per document (seeded by id), as with a fixed seed.
        let h = d.id.bytes().fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(b as u64));
        value = Some((h % 1_000_000) as f64 / 1_000_000.0);
    }
    Ok(match (value, weight) {
        (Some(v), Some(w)) => Some(v * w),
        (Some(v), None) => Some(v),
        (None, Some(w)) => Some(w),
        (None, None) => None,
    })
}

/// `function_score`.
pub fn function_score(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let query = v.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}));
    let base = eval(&query, mappings, docs)?;
    // A top-level function (no `functions` array) is one function.
    let mut funcs: Vec<Map<String, Value>> = match v.get("functions").and_then(Value::as_array) {
        Some(a) => a.iter().filter_map(|f| f.as_object().cloned()).collect(),
        None => vec![],
    };
    if funcs.is_empty() {
        let o = v.as_object().cloned().unwrap_or_default();
        let keys = [
            "field_value_factor",
            "gauss",
            "exp",
            "linear",
            "script_score",
            "random_score",
            "weight",
        ];
        if keys.iter().any(|k| o.contains_key(*k)) {
            funcs.push(o.into_iter().filter(|(k, _)| keys.contains(&k.as_str())).collect());
        }
    }
    let filters: Vec<Option<Scores>> = funcs
        .iter()
        .map(|f| f.get("filter").map(|q| eval(q, mappings, docs)).transpose())
        .collect::<Result<_, _>>()?;
    let score_mode = v.get("score_mode").and_then(Value::as_str).unwrap_or("multiply");
    let boost_mode = v.get("boost_mode").and_then(Value::as_str).unwrap_or("multiply");
    let max_boost = v.get("max_boost").and_then(Value::as_f64).unwrap_or(f64::MAX);
    let min_score = v.get("min_score").and_then(Value::as_f64);
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0);
    let mut out = Scores::new();
    for (i, q) in base {
        let d = &docs[i];
        let mut vals: Vec<(f64, f64)> = vec![];
        for (f, filt) in funcs.iter().zip(&filters) {
            if filt.as_ref().is_some_and(|m| !m.contains_key(&i)) {
                continue;
            }
            if let Some(x) = function_value(f, mappings, d, q)? {
                vals.push((x, f.get("weight").and_then(Value::as_f64).unwrap_or(1.0)));
            }
        }
        let fs = if vals.is_empty() {
            1.0
        } else {
            match score_mode {
                "sum" => vals.iter().map(|v| v.0).sum(),
                "avg" => {
                    vals.iter().map(|v| v.0).sum::<f64>() / vals.iter().map(|v| v.1).sum::<f64>()
                }
                "first" => vals[0].0,
                "max" => vals.iter().map(|v| v.0).fold(f64::MIN, f64::max),
                "min" => vals.iter().map(|v| v.0).fold(f64::MAX, f64::min),
                _ => vals.iter().map(|v| v.0).product(),
            }
        }
        .min(max_boost);
        let q = q as f64;
        let s = match boost_mode {
            "replace" => fs,
            "sum" => q + fs,
            "avg" => (q + fs) / 2.0,
            "max" => q.max(fs),
            "min" => q.min(fs),
            _ => q * fs,
        } * boost;
        if min_score.is_some_and(|m| s < m) {
            continue;
        }
        out.insert(i, s as f32);
    }
    Ok(out)
}

/// The `doc` a scoring script sees: each field's values, `.value` the
/// first.
fn doc_view(mappings: &Value, d: &CommittedDoc, src: &str) -> Value {
    let mut m = Map::new();
    // Only the fields the script names (`doc['x']`).
    let mut rest = src;
    while let Some(i) = rest.find("doc[") {
        rest = &rest[i + 4..];
        let q = rest.chars().next().unwrap_or('\'');
        let name: String = rest[1..].chars().take_while(|c| *c != q).collect();
        let is_date =
            matches!(resolve_field(mappings, &name).1.as_deref(), Some("date" | "date_nanos"));
        let vals: Vec<Value> = if is_date {
            numbers(mappings, d, &name).into_iter().map(|n| json!(n as i64)).collect()
        } else {
            raw_values(&d.source, &name).into_iter().cloned().collect()
        };
        m.insert(
            name,
            json!({"value": vals.first().cloned().unwrap_or(Value::Null), "values": vals, "length": vals.len(), "empty": vals.is_empty()}),
        );
    }
    Value::Object(m)
}

fn run_score_script(
    script: &Value,
    mappings: &Value,
    d: &CommittedDoc,
    score: f32,
) -> Result<f64, EsError> {
    let (src, params) = painless::script_parts(script).map_err(|e| EsError::parsing(&e))?;
    let compiled = painless::compile(&src)
        .map_err(|e| EsError::new(400, "script_exception", &format!("compile error: {e}")))?;
    let mut vars = Map::new();
    vars.insert("doc".into(), doc_view(mappings, d, &src));
    vars.insert("_score".into(), json!(score));
    vars.insert("params".into(), params);
    let v = compiled
        .value(vars)
        .map_err(|e| EsError::new(400, "script_exception", &format!("runtime error: {e}")))?;
    v.as_f64().ok_or_else(|| {
        EsError::new(400, "script_exception", "script score function must return a number")
    })
}

/// `script_score`.
pub fn script_score(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let script =
        v.get("script").ok_or_else(|| EsError::parsing("[script_score] requires a [script]"))?;
    if !script.is_object() {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            "[script_score] failed to parse field [script]",
        ));
    }
    let query =
        v.get("query").ok_or_else(|| EsError::parsing("[script_score] requires a [query]"))?;
    let min_score = v.get("min_score").and_then(Value::as_f64);
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0);
    let mut out = Scores::new();
    for (i, q) in eval(query, mappings, docs)? {
        let s = run_score_script(script, mappings, &docs[i], q)?;
        if s < 0.0 {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                &format!(
                    "script_score script returned an invalid score [{s}] for doc [{i}]. Must be a non-negative score!"
                ),
            ));
        }
        let s = s * boost;
        if min_score.is_some_and(|m| s < m) {
            continue;
        }
        out.insert(i, s as f32);
    }
    Ok(out)
}

/// `combined_fields`: the fields scored as one (BM25F with unit weights).
pub fn combined_fields(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let text = query_text(v.get("query"));
    let fields: Vec<String> = v
        .get("fields")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .flat_map(|f| {
                    super::highlight::expand_field_pattern(
                        mappings,
                        f.split('^').next().unwrap_or(f),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if fields.is_empty() {
        return Err(EsError::parsing("[combined_fields] query requires 'fields' to be set"));
    }
    let and =
        v.get("operator").and_then(Value::as_str).is_some_and(|o| o.eq_ignore_ascii_case("and"));
    let terms = analyze_for(mappings, &fields[0], &text);
    // One combined token list per document.
    let per_field: Vec<Vec<Vec<String>>> =
        fields.iter().map(|f| doc_tokens(mappings, docs, f)).collect();
    let combined: Vec<Vec<String>> = (0..docs.len())
        .map(|i| per_field.iter().flat_map(|f| f[i].iter().cloned()).collect())
        .collect();
    let doc_count = combined.iter().filter(|t| !t.is_empty()).count().max(1) as u64;
    let total: u64 = combined.iter().map(|t| t.len() as u64).sum();
    let avg = total as f32 / doc_count as f32;
    let mut out = Scores::new();
    let mut matched: HashMap<usize, HashSet<&String>> = HashMap::new();
    for t in &terms {
        let df = combined.iter().filter(|toks| toks.contains(t)).count() as u64;
        if df == 0 {
            continue;
        }
        for (i, toks) in combined.iter().enumerate() {
            let tf = toks.iter().filter(|x| *x == t).count() as u32;
            if tf == 0 {
                continue;
            }
            let len = super::scoring::norm_doc_len(toks.len() as u32).max(1);
            *out.entry(i).or_insert(0.0) += super::scoring::score(tf, len, avg, df, doc_count);
            matched.entry(i).or_default().insert(t);
        }
    }
    if and {
        let need: HashSet<&String> = terms.iter().collect();
        out.retain(|i, _| matched.get(i).is_some_and(|m| m.len() == need.len()));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Geo

/// Mean earth radius Elasticsearch uses (GeoUtils.EARTH_MEAN_RADIUS), m.
const EARTH_RADIUS: f64 = 6_371_008.771_4;

/// A geo point in any of Elasticsearch's forms: `{lat, lon}`, `"lat,lon"`,
/// `[lon, lat]`, `"POINT (lon lat)"`.
pub fn parse_point(v: &Value) -> Option<(f64, f64)> {
    match v {
        Value::Object(o) => {
            let lat = o.get("lat")?.as_f64().or_else(|| o.get("lat")?.as_str()?.parse().ok())?;
            let lon = o.get("lon")?.as_f64().or_else(|| o.get("lon")?.as_str()?.parse().ok())?;
            Some((lat, lon))
        }
        Value::Array(a) if a.len() >= 2 => Some((a[1].as_f64()?, a[0].as_f64()?)),
        Value::String(s) => {
            let t = s.trim();
            if let Some(rest) = t.strip_prefix("POINT").or_else(|| t.strip_prefix("point")) {
                let inner = rest.trim().trim_start_matches('(').trim_end_matches(')');
                let mut it = inner.split_whitespace();
                let lon: f64 = it.next()?.parse().ok()?;
                let lat: f64 = it.next()?.parse().ok()?;
                return Some((lat, lon));
            }
            let (a, b) = t.split_once(',')?;
            Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
        }
        _ => None,
    }
}

/// The value at a dotted path, unflattened (a `[lon, lat]` array stays
/// one value).
fn at_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if let Some(x) = v.get(path) {
        return Some(x);
    }
    let (head, rest) = path.split_once('.')?;
    at_path(v.get(head)?, rest)
}

/// A document's points for `field` (a single point or an array of them).
fn points(d: &CommittedDoc, field: &str) -> Vec<(f64, f64)> {
    let Some(v) = at_path(&d.source, field) else { return vec![] };
    match v {
        // `[lon, lat]` is one point; an array of points is several.
        Value::Array(a) if a.len() == 2 && a.iter().all(Value::is_number) => {
            parse_point(v).into_iter().collect()
        }
        Value::Array(a) => a.iter().filter_map(parse_point).collect(),
        other => parse_point(other).into_iter().collect(),
    }
}

/// Haversine distance in meters.
pub fn distance_m(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (la1, lo1, la2, lo2) =
        (a.0.to_radians(), a.1.to_radians(), b.0.to_radians(), b.1.to_radians());
    let h = ((la2 - la1) / 2.0).sin().powi(2)
        + la1.cos() * la2.cos() * ((lo2 - lo1) / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS * h.sqrt().asin()
}

/// A distance with its unit (`12km`, `500`, `3mi`) in meters.
pub fn parse_distance(v: &Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    let s = v.as_str()?.trim();
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-').unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: f64 = n.parse().ok()?;
    Some(n * unit_meters(unit.trim())?)
}

pub fn unit_meters(unit: &str) -> Option<f64> {
    Some(match unit {
        "" | "m" | "meters" => 1.0,
        "km" | "kilometers" => 1000.0,
        "cm" | "centimeters" => 0.01,
        "mm" | "millimeters" => 0.001,
        "mi" | "miles" => 1609.344,
        "yd" | "yards" => 0.9144,
        "ft" | "feet" => 0.3048,
        "in" | "inch" => 0.0254,
        "nmi" | "NM" | "nauticalmiles" => 1852.0,
        _ => return None,
    })
}

fn geo_field(v: &Value, skip: &[&str]) -> Option<(String, Value)> {
    v.as_object()?
        .iter()
        .find(|(k, _)| !skip.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
}

/// `geo_distance`.
pub fn geo_distance(v: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let skip =
        ["distance", "distance_type", "validation_method", "_name", "boost", "ignore_unmapped"];
    let (field, center) =
        geo_field(v, &skip).ok_or_else(|| EsError::parsing("[geo_distance] requires a field"))?;
    let center = parse_point(&center)
        .ok_or_else(|| EsError::parsing("[geo_distance] failed to parse point"))?;
    let max = v
        .get("distance")
        .and_then(parse_distance)
        .ok_or_else(|| EsError::parsing("[geo_distance] requires 'distance' to be specified"))?;
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    Ok(docs
        .iter()
        .enumerate()
        .filter(|(_, d)| points(d, &field).into_iter().any(|p| distance_m(center, p) <= max))
        .map(|(i, _)| (i, boost))
        .collect())
}

/// `geo_bounding_box`.
pub fn geo_bounding_box(v: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let skip = ["validation_method", "_name", "boost", "ignore_unmapped", "type"];
    let (field, b) =
        geo_field(v, &skip).ok_or_else(|| EsError::parsing("[geo_bbox] requires a field"))?;
    let bad = || EsError::parsing("failed to parse bounding box");
    let (top, left, bottom, right) =
        if let (Some(tl), Some(br)) = (b.get("top_left"), b.get("bottom_right")) {
            let tl = parse_point(tl).ok_or_else(bad)?;
            let br = parse_point(br).ok_or_else(bad)?;
            (tl.0, tl.1, br.0, br.1)
        } else if let (Some(tr), Some(bl)) = (b.get("top_right"), b.get("bottom_left")) {
            let tr = parse_point(tr).ok_or_else(bad)?;
            let bl = parse_point(bl).ok_or_else(bad)?;
            (tr.0, bl.1, bl.0, tr.1)
        } else {
            let g = |k: &str| b.get(k).and_then(Value::as_f64);
            (
                g("top").ok_or_else(bad)?,
                g("left").ok_or_else(bad)?,
                g("bottom").ok_or_else(bad)?,
                g("right").ok_or_else(bad)?,
            )
        };
    let boost = v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let inside = |(lat, lon): (f64, f64)| {
        let lon_ok =
            if left <= right { lon >= left && lon <= right } else { lon >= left || lon <= right };
        lat <= top && lat >= bottom && lon_ok
    };
    Ok(docs
        .iter()
        .enumerate()
        .filter(|(_, d)| points(d, &field).into_iter().any(inside))
        .map(|(i, _)| (i, boost))
        .collect())
}

/// `_geo_distance` sort key: the distance (in `unit`) from `origin` to
/// the document's nearest (`asc`) or farthest (`desc`) point.
pub fn sort_distance(
    d: &CommittedDoc,
    field: &str,
    origin: (f64, f64),
    unit_m: f64,
    desc: bool,
) -> Option<f64> {
    let ds = points(d, field).into_iter().map(|p| distance_m(origin, p) / unit_m);
    if desc {
        ds.fold(None, |m: Option<f64>, x| Some(m.map_or(x, |m| m.max(x))))
    } else {
        ds.fold(None, |m: Option<f64>, x| Some(m.map_or(x, |m| m.min(x))))
    }
}
