//! `_termvectors` / `_mtermvectors`: a document's terms per field, with
//! their frequencies, positions and offsets, plus field and term
//! statistics over the documents of the shard holding it. Fields mapped
//! with `term_vector` are returned by default; any other text or keyword
//! field is analyzed on the fly when asked for by name (as Elasticsearch
//! generates term vectors for fields that don't store them).

use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};

use super::analysis;
use super::search::raw_values;

/// A term vectors request's options (URL parameters, body fields, or an
/// `_mtermvectors` item's fields over the request-wide `parameters`).
#[derive(Clone)]
pub struct Options {
    pub fields: Option<Vec<String>>,
    pub field_statistics: bool,
    pub term_statistics: bool,
    pub positions: bool,
    pub offsets: bool,
    pub payloads: bool,
    pub realtime: bool,
    pub routing: Option<String>,
    pub version: Option<i64>,
    pub per_field_analyzer: Map<String, Value>,
    /// An artificial document to analyze instead of a stored one.
    pub doc: Option<Value>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            fields: None,
            field_statistics: true,
            term_statistics: false,
            positions: true,
            offsets: true,
            payloads: true,
            realtime: true,
            routing: None,
            version: None,
            per_field_analyzer: Map::new(),
            doc: None,
        }
    }
}

/// Request body / item keys Elasticsearch's parser accepts.
const KEYS: &[&str] = &[
    "fields",
    "offsets",
    "positions",
    "payloads",
    "dfs",
    "term_statistics",
    "field_statistics",
    "_index",
    "_id",
    "doc",
    "routing",
    "version",
    "version_type",
    "per_field_analyzer",
    "filter",
    "realtime",
    "preference",
];

fn truthy(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) => Some(s != "false"),
        _ => None,
    }
}

fn list(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => s.split(',').map(|f| f.trim().to_string()).collect(),
        Value::Array(a) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        _ => vec![],
    }
}

/// URL parameters the term vectors APIs take.
const PARAMS: &[&str] = &[
    "fields",
    "field_statistics",
    "offsets",
    "payloads",
    "positions",
    "term_statistics",
    "routing",
    "realtime",
    "version",
    "version_type",
    "preference",
    "ids",
    "pretty",
    "human",
    "error_trace",
    "filter_path",
];

/// A URL parameter the API doesn't take, as Elasticsearch names it.
pub fn unknown_param(q: &HashMap<String, String>) -> Option<&str> {
    let mut bad: Vec<&str> = q.keys().map(String::as_str).filter(|k| !PARAMS.contains(k)).collect();
    bad.sort();
    bad.first().copied()
}

impl Options {
    /// URL parameters over the defaults.
    pub fn from_params(q: &HashMap<String, String>) -> Self {
        let mut o = Self::default();
        let flag = |k: &str, d: bool| q.get(k).map_or(d, |v| v != "false");
        o.fields = q.get("fields").map(|f| list(&json!(f)));
        o.field_statistics = flag("field_statistics", true);
        o.term_statistics = flag("term_statistics", false);
        o.positions = flag("positions", true);
        o.offsets = flag("offsets", true);
        o.payloads = flag("payloads", true);
        o.realtime = flag("realtime", true);
        o.routing = q.get("routing").cloned();
        o.version = q.get("version").and_then(|v| v.parse().ok());
        o
    }

    /// Body (or item) fields over these options; unknown fields are an
    /// error, as Elasticsearch's parser reports them.
    pub fn apply_body(&mut self, body: &Value) -> Result<(), String> {
        let Some(m) = body.as_object() else { return Ok(()) };
        for (k, v) in m {
            match k.as_str() {
                "fields" => self.fields = Some(list(v)),
                "field_statistics" => self.field_statistics = truthy(v).unwrap_or(true),
                "term_statistics" => self.term_statistics = truthy(v).unwrap_or(false),
                "positions" => self.positions = truthy(v).unwrap_or(true),
                "offsets" => self.offsets = truthy(v).unwrap_or(true),
                "payloads" => self.payloads = truthy(v).unwrap_or(true),
                "realtime" => self.realtime = truthy(v).unwrap_or(true),
                "routing" => {
                    self.routing = v.as_str().map(str::to_string).or_else(|| Some(v.to_string()))
                }
                "version" => self.version = v.as_i64(),
                "per_field_analyzer" => {
                    self.per_field_analyzer = v.as_object().cloned().unwrap_or_default()
                }
                "doc" => self.doc = Some(v.clone()),
                k if KEYS.contains(&k) => {}
                other => {
                    return Err(format!(
                        "failed to parse term vectors request. unknown field [{other}]"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// A mapped field: its full name, `_source` path and definition.
fn mapped_fields(props: &Value, prefix: &str, out: &mut Vec<(String, String, Value)>) {
    let Some(m) = props.as_object() else { return };
    for (k, node) in m {
        let name = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        if let Some(p) = node.get("properties") {
            mapped_fields(p, &name, out);
            continue;
        }
        if let Some(Value::Object(subs)) = node.get("fields") {
            for (sk, sn) in subs {
                out.push((format!("{name}.{sk}"), name.clone(), sn.clone()));
            }
        }
        out.push((name.clone(), name, node.clone()));
    }
}

fn glob(pat: &str, name: &str) -> bool {
    match pat.split_once('*') {
        None => pat == name,
        Some((pre, rest)) => {
            name.starts_with(pre) && {
                let tail = &name[pre.len()..];
                rest.is_empty() || (0..=tail.len()).any(|i| glob(rest, &tail[i..]))
            }
        }
    }
}

/// Field types whose values are indexed as terms (numbers, dates and the
/// like are points: no term vectors).
fn has_terms(ty: &str) -> bool {
    matches!(
        ty,
        "text"
            | "match_only_text"
            | "search_as_you_type"
            | "keyword"
            | "constant_keyword"
            | "wildcard"
            | "flattened"
    )
}

fn is_text(ty: &str) -> bool {
    matches!(ty, "text" | "match_only_text" | "search_as_you_type")
}

/// One analyzed token: term, position, character offsets.
type Token = (String, usize, usize, usize);

/// `text`'s tokens for `field`, with positions and offsets, as the
/// analysis engine produces them (the field's index analyzer, or
/// `analyzer`); a non-text field's value is one token (after a keyword
/// field's normalizer).
fn tokens(
    mappings: &Value,
    field: &str,
    ty: &str,
    text: &str,
    analyzer: Option<&str>,
) -> Vec<Token> {
    let toks = match analyzer {
        Some(a) => analysis::analyzer(a).tokens(text),
        None if is_text(ty) => analysis::field_tokens(mappings, field, text, analysis::Mode::Index),
        None => {
            let term = analysis::normalize(mappings, field, text);
            return vec![(term, 0, 0, text.chars().count())];
        }
    };
    let positions = analysis::positions(&toks);
    toks.into_iter()
        .zip(positions)
        .map(|(t, pos)| (t.term, pos.max(0) as usize, t.start, t.end))
        .collect()
}

/// All tokens of a field's values (arrays analyzed as one field: text
/// positions jump by `position_increment_gap`, offsets continue past
/// each value).
fn field_tokens(
    mappings: &Value,
    name: &str,
    path: &str,
    def: &Value,
    source: &Value,
    analyzer: Option<&str>,
) -> Vec<Token> {
    let ty = def.get("type").and_then(Value::as_str).unwrap_or("text");
    let gap = def.get("position_increment_gap").and_then(Value::as_u64).unwrap_or(100) as usize;
    let mut out = vec![];
    let (mut pos_base, mut off_base) = (0, 0);
    for v in raw_values(source, path) {
        let text = match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        if let Some(limit) = def.get("ignore_above").and_then(Value::as_u64)
            && text.chars().count() as u64 > limit
        {
            continue;
        }
        let toks = tokens(mappings, name, ty, &text, analyzer);
        let last = toks.iter().map(|t| t.1).max();
        for (t, p, s, e) in toks {
            out.push((t, p + pos_base, s + off_base, e + off_base));
        }
        if let Some(l) = last {
            pos_base += l + 1 + if is_text(ty) { gap } else { 0 };
        }
        off_base += text.chars().count() + 1;
    }
    out
}

/// The `term_vectors` object of `source`: per selected field, its terms
/// (and statistics over `stats_docs`, the sources of the shard's
/// documents).
pub fn term_vectors(mappings: &Value, source: &Value, stats_docs: &[&Value], o: &Options) -> Value {
    let mut all = vec![];
    if let Some(p) = mappings.get("properties") {
        mapped_fields(p, "", &mut all);
    }
    let selected: Vec<&(String, String, Value)> = all
        .iter()
        .filter(|(name, _, def)| {
            let ty = def.get("type").and_then(Value::as_str).unwrap_or("object");
            if !has_terms(ty) {
                return false;
            }
            match &o.fields {
                Some(fs) => fs.iter().any(|f| glob(f, name)),
                // Without `fields`: the fields storing term vectors (an
                // artificial document has none stored: all of them).
                None => {
                    o.doc.is_some()
                        || def
                            .get("term_vector")
                            .and_then(Value::as_str)
                            .is_some_and(|tv| tv != "no")
                }
            }
        })
        .collect();
    let mut out = Map::new();
    for (name, path, def) in selected {
        let analyzer = o.per_field_analyzer.get(name).and_then(Value::as_str);
        let toks = field_tokens(mappings, name, path, def, source, analyzer);
        if toks.is_empty() {
            continue;
        }
        // A stored term vector keeps only what its mapping asked for.
        let stored = def.get("term_vector").and_then(Value::as_str).unwrap_or("");
        let generated = o.fields.is_some() || stored.is_empty() || stored == "no";
        let keep_pos = o.positions && (generated || stored.contains("positions"));
        let keep_off = o.offsets && (generated || stored.contains("offsets"));
        let mut terms: BTreeMap<String, Vec<(usize, usize, usize)>> = BTreeMap::new();
        for (t, p, s, e) in toks {
            terms.entry(t).or_default().push((p, s, e));
        }
        let mut field = Map::new();
        // Statistics over the shard's documents having the field.
        let docs_terms: Vec<HashMap<String, usize>> = stats_docs
            .iter()
            .map(|src| {
                let mut m = HashMap::new();
                for (t, ..) in field_tokens(mappings, name, path, def, src, analyzer) {
                    *m.entry(t).or_insert(0) += 1;
                }
                m
            })
            .filter(|m| !m.is_empty())
            .collect();
        if o.field_statistics {
            let sum_doc_freq: usize = docs_terms.iter().map(HashMap::len).sum();
            let sum_ttf: usize = docs_terms.iter().flat_map(|m| m.values()).sum();
            field.insert(
                "field_statistics".into(),
                json!({"sum_doc_freq": sum_doc_freq, "doc_count": docs_terms.len(), "sum_ttf": sum_ttf}),
            );
        }
        let mut tmap = Map::new();
        for (t, occ) in terms {
            let mut entry = Map::new();
            if o.term_statistics {
                let doc_freq = docs_terms.iter().filter(|m| m.contains_key(&t)).count();
                if doc_freq > 0 {
                    let ttf: usize = docs_terms.iter().filter_map(|m| m.get(&t)).sum();
                    entry.insert("doc_freq".into(), json!(doc_freq));
                    entry.insert("ttf".into(), json!(ttf));
                }
            }
            entry.insert("term_freq".into(), json!(occ.len()));
            if keep_pos || keep_off {
                let toks: Vec<Value> = occ
                    .iter()
                    .map(|(p, s, e)| {
                        let mut tok = Map::new();
                        if keep_pos {
                            tok.insert("position".into(), json!(p));
                        }
                        if keep_off {
                            tok.insert("start_offset".into(), json!(s));
                            tok.insert("end_offset".into(), json!(e));
                        }
                        Value::Object(tok)
                    })
                    .collect();
                entry.insert("tokens".into(), Value::Array(toks));
            }
            tmap.insert(t, Value::Object(entry));
        }
        field.insert("terms".into(), Value::Object(tmap));
        out.insert(name.clone(), Value::Object(field));
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terms_offsets_and_statistics() {
        let m = json!({"properties": {"text": {"type": "text", "term_vector": "with_positions_offsets"}}});
        let src = json!({"text": "The quick brown fox is brown."});
        let o = Options { term_statistics: true, ..Options::default() };
        let tv = term_vectors(&m, &src, &[&src], &o);
        let t = &tv["text"];
        assert_eq!(t["field_statistics"], json!({"sum_doc_freq": 5, "doc_count": 1, "sum_ttf": 6}));
        assert_eq!(t["terms"]["brown"]["term_freq"], 2);
        assert_eq!(t["terms"]["brown"]["doc_freq"], 1);
        assert_eq!(
            t["terms"]["brown"]["tokens"][0],
            json!({"position": 2, "start_offset": 10, "end_offset": 15})
        );
        // Not stored and not asked for: nothing.
        let m2 = json!({"properties": {"text": {"type": "text"}}});
        assert_eq!(term_vectors(&m2, &src, &[], &Options::default()), json!({}));
        let o = Options { fields: Some(vec!["text".into()]), ..Options::default() };
        assert_eq!(term_vectors(&m2, &src, &[], &o)["text"]["terms"]["fox"]["term_freq"], 1);
    }

    #[test]
    fn unknown_item_fields_are_refused() {
        let mut o = Options::default();
        assert!(o.apply_body(&json!({"versionType": "external"})).is_err());
        assert!(o.apply_body(&json!({"fields": ["a"], "routing": "5"})).is_ok());
        assert_eq!(o.routing.as_deref(), Some("5"));
    }
}
