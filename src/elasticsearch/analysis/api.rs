//! The `_analyze` API: run text through a named analyzer, a field's
//! analyzer, a normalizer, or a chain given in the request, with
//! Elasticsearch's token output (and `explain` detail).

use std::rc::Rc;

use serde_json::{Map, Value, json};

use super::registry::{Chain, Defs};
use super::token::Token;
use super::tokenizers::Tokenizer;
use super::{AnalysisError, Analyzer, field_def, lookup, with_defs};

fn iae(reason: impl Into<String>) -> AnalysisError {
    AnalysisError::new("illegal_argument_exception", reason)
}

/// What an index contributes to an `_analyze` request.
pub struct IndexInfo<'a> {
    pub mappings: &'a Value,
    pub max_token_count: usize,
}

const FIELDS: &[&str] = &[
    "text",
    "analyzer",
    "tokenizer",
    "filter",
    "token_filter",
    "char_filter",
    "normalizer",
    "field",
    "explain",
    "attributes",
];

/// `POST [/{index}]/_analyze`.
pub fn analyze(req: &Value, index: Option<IndexInfo>) -> Result<Value, AnalysisError> {
    let Some(obj) = req.as_object() else {
        return Err(AnalysisError::new(
            "parse_exception",
            "request body or source parameter is required",
        ));
    };
    if let Some(k) = obj.keys().find(|k| !FIELDS.contains(&k.as_str())) {
        return Err(AnalysisError::new(
            "x_content_parse_exception",
            format!("[1:2] [analyze_request] unknown field [{k}]"),
        ));
    }
    let texts: Vec<String> = match obj.get("text") {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => {
            a.iter().map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string)).collect()
        }
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other.to_string()],
    };
    let s = |k: &str| obj.get(k).and_then(Value::as_str);
    let analyzer_name = s("analyzer");
    let normalizer_name = s("normalizer");
    let tokenizer = obj.get("tokenizer").filter(|v| !v.is_null());
    let filters: Vec<&Value> = obj
        .get("filter")
        .or_else(|| obj.get("token_filter"))
        .and_then(Value::as_array)
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    let char_filters: Vec<&Value> = obj
        .get("char_filter")
        .and_then(Value::as_array)
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    let mut problems = Vec::new();
    if texts.is_empty() {
        problems.push("text is missing");
    }
    if index.is_none() && normalizer_name.is_some() {
        problems.push("index is required if normalizer is specified");
    }
    if normalizer_name.is_some() && (tokenizer.is_some() || analyzer_name.is_some()) {
        problems.push("tokenizer/analyze should be null if normalizer is specified");
    }
    if analyzer_name.is_some()
        && (tokenizer.is_some() || !filters.is_empty() || !char_filters.is_empty())
    {
        problems.push("cannot define extra components on a named analyzer");
    }
    if !problems.is_empty() {
        let list: String = problems
            .iter()
            .enumerate()
            .map(|(i, p)| format!("{}: {p};", i + 1))
            .collect::<Vec<_>>()
            .join("");
        return Err(AnalysisError::new(
            "action_request_validation_exception",
            format!("Validation Failed: {list}"),
        ));
    }
    let explain =
        obj.get("explain").is_some_and(|v| v.as_bool() == Some(true) || v.as_str() == Some("true"));
    let attributes: Option<Vec<String>> = obj
        .get("attributes")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect());
    let global = if index.is_some() { "" } else { "global " };

    // Which analyzer, and the position gap between values.
    let (analyzer, gap): (Rc<Analyzer>, u32) = if let Some(name) = analyzer_name {
        match lookup(name, false) {
            Some(r) => {
                let a = r?;
                let g = a.position_increment_gap;
                (a, g)
            }
            None => return Err(iae(format!("failed to find {global}analyzer [{name}]"))),
        }
    } else if let Some(name) = normalizer_name {
        match lookup(name, true) {
            Some(r) => (r?, 0),
            None => return Err(iae(format!("failed to find normalizer under [{name}]"))),
        }
    } else if tokenizer.is_some() || !filters.is_empty() || !char_filters.is_empty() {
        // Explain output runs a request's own chain without a gap.
        let gap = if explain { 0 } else { 100 };
        (Rc::new(request_chain(tokenizer, &filters, &char_filters, global)?), gap)
    } else if let (Some(field), Some(info)) = (s("field"), index.as_ref()) {
        field_analysis(info.mappings, field)?
    } else {
        let a = with_defs(|defs, _| {
            defs.analysis.get("analyzer").and_then(|a| a.get("default")).is_some()
        });
        let name = if a { "default" } else { "standard" };
        match lookup(name, false) {
            Some(r) => {
                let a = r?;
                let g = a.position_increment_gap;
                (a, g)
            }
            None => return Err(iae(format!("failed to find {global}analyzer [{name}]"))),
        }
    };
    let max_tokens = index.as_ref().map_or(10_000, |i| i.max_token_count);
    let too_many = || {
        AnalysisError::with_status(
            "illegal_state_exception",
            format!(
                "The number of tokens produced by calling _analyze has exceeded the allowed maximum of [{max_tokens}]. This limit can be set by changing the [index.analyze.max_token_count] index level setting."
            ),
            500,
        )
    };

    if !explain {
        let mut acc = Accumulator::default();
        for t in &texts {
            acc.add(t, analyzer.tokens(t), gap);
            if acc.out.len() > max_tokens {
                return Err(too_many());
            }
        }
        let tokens: Vec<Value> = acc.out.iter().map(|(t, pos)| token_json(t, *pos, None)).collect();
        return Ok(json!({"tokens": tokens}));
    }

    // Explain: every stage, each accumulated over the values.
    let stages: Vec<super::Stages> = texts.iter().map(|t| analyzer.stages(t)).collect();
    let attrs = |keyword: bool| Attrs { keyword, only: attributes.clone() };
    let stage_tokens = |pick: &dyn Fn(&super::Stages) -> Vec<Token>,
                        keyword: bool|
     -> Result<Vec<Value>, AnalysisError> {
        let mut acc = Accumulator::default();
        for (t, st) in texts.iter().zip(&stages) {
            acc.add(t, pick(st), gap);
            if acc.out.len() > max_tokens {
                return Err(too_many());
            }
        }
        let a = attrs(keyword);
        Ok(acc.out.iter().map(|(t, pos)| token_json(t, *pos, Some(&a))).collect())
    };
    let mut detail = Map::new();
    detail.insert("custom_analyzer".into(), json!(analyzer.custom));
    if analyzer.custom {
        let cfs: Vec<Value> = analyzer
            .char_filters
            .iter()
            .enumerate()
            .map(|(i, (name, _))| {
                json!({"name": name, "filtered_text": stages.iter().map(|s| s.char_filters[i].clone()).collect::<Vec<_>>()})
            })
            .collect();
        detail.insert("charfilters".into(), json!(cfs));
        let toks = stage_tokens(&|s| s.tokenizer.clone(), false)?;
        detail.insert("tokenizer".into(), json!({"name": analyzer.tokenizer.0, "tokens": toks}));
        let mut tfs = Vec::new();
        let mut keyword = false;
        for (i, (name, f)) in analyzer.filters.iter().enumerate() {
            keyword |= f.sets_keyword_attr();
            let toks = stage_tokens(&|s| s.filters[i].clone(), keyword)?;
            tfs.push(json!({"name": name, "tokens": toks}));
        }
        detail.insert("tokenfilters".into(), json!(tfs));
    } else {
        let keyword = analyzer.filters.iter().any(|(_, f)| f.sets_keyword_attr());
        let toks = stage_tokens(
            &|s| s.filters.last().cloned().unwrap_or_else(|| s.tokenizer.clone()),
            keyword,
        )?;
        detail.insert("analyzer".into(), json!({"name": analyzer.name, "tokens": toks}));
    }
    Ok(json!({"detail": Value::Object(detail)}))
}

/// The analyzer `_analyze` uses for `field` of an index.
fn field_analysis(mappings: &Value, field: &str) -> Result<(Rc<Analyzer>, u32), AnalysisError> {
    let def = field_def(mappings, field);
    let ty = def.and_then(|d| d.get("type")).and_then(Value::as_str).unwrap_or("text");
    let gap = |d: Option<&Value>| {
        d.and_then(|d| d.get("position_increment_gap"))
            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(100) as u32
    };
    match ty {
        "text" | "match_only_text" | "search_as_you_type" | "annotated_text" => {
            let name = super::field_analyzer_name(mappings, field, super::Mode::Index);
            Ok((super::analyzer(&name), gap(def)))
        }
        "keyword" | "constant_keyword" | "wildcard" => {
            match def.and_then(|d| d.get("normalizer")).and_then(Value::as_str) {
                Some(n) => match lookup(n, true) {
                    Some(r) => Ok((r?, 0)),
                    None => Err(iae(format!("failed to find normalizer under [{n}]"))),
                },
                None => Ok((super::analyzer("keyword"), 0)),
            }
        }
        _ => Err(iae(format!(
            "Can't process field [{field}], Analysis requests are only supported on tokenized fields"
        ))),
    }
}

/// A chain from the request's `tokenizer` / `filter` / `char_filter`
/// (names or inline definitions); without a tokenizer it is a
/// normalizer.
fn request_chain(
    tokenizer: Option<&Value>,
    filters: &[&Value],
    char_filters: &[&Value],
    global: &str,
) -> Result<Analyzer, AnalysisError> {
    with_defs(|defs: &Defs, _| {
        let anon = |v: &Value| {
            format!("__anonymous__{}", v.get("type").and_then(Value::as_str).unwrap_or(""))
        };
        let (tname, tok) = match tokenizer {
            None => ("keyword".to_string(), Tokenizer::Keyword),
            Some(Value::String(n)) => match defs.tokenizer_named(n) {
                Some(t) => (n.clone(), t?),
                None => return Err(iae(format!("failed to find {global}tokenizer under [{n}]"))),
            },
            Some(def) => (anon(def), inline(defs.tokenizer_def(&anon(def), def), "tokenizer", def, global)?),
        };
        let normalizer = tokenizer.is_none();
        let mut chain = Chain::new(&tname, tok);
        for cf in char_filters {
            let (name, c) = match cf {
                Value::String(n) => match defs.char_filter_named(n) {
                    Some(c) => (n.clone(), c?),
                    None => {
                        return Err(iae(format!("failed to find {global}char_filter under [{n}]")));
                    }
                },
                def => (anon(def), inline(defs.char_filter_def(&anon(def), def), "char_filter", def, global)?),
            };
            chain.char_filters.push((name, c));
        }
        for f in filters {
            let (name, tf) = match f {
                Value::String(n) => match defs.filter_named(n, &chain) {
                    Some(t) => (n.clone(), t?),
                    None => return Err(iae(format!("failed to find {global}filter under [{n}]"))),
                },
                def => (anon(def), inline(defs.filter_def(&anon(def), def, &chain), "filter", def, global)?),
            };
            if normalizer && !normalizing(&tf) {
                return Err(iae(format!("Custom normalizer may not use filter [{name}]")));
            }
            chain.filters.push((name, tf));
        }
        Ok(chain.into_analyzer("", true, 100))
    })
}

/// An inline definition's result: an unknown `type` reads as a name
/// that isn't found.
fn inline<T>(r: Result<T, AnalysisError>, kind: &str, def: &Value, global: &str) -> Result<T, AnalysisError> {
    r.map_err(|e| {
        if e.reason.starts_with("Unknown ") {
            let ty = def.get("type").and_then(Value::as_str).unwrap_or("");
            iae(format!("failed to find {global}{kind} under [{ty}]"))
        } else {
            e
        }
    })
}

fn normalizing(f: &super::filters::TokenFilter) -> bool {
    use super::filters::TokenFilter::*;
    matches!(
        f,
        Lowercase(_)
            | Uppercase
            | AsciiFolding { .. }
            | CjkWidth
            | DecimalDigit
            | Elision { .. }
            | GermanNormalization
            | PatternReplace { .. }
            | Trim
    )
}

/// Tokens of several values, with Elasticsearch's running position and
/// offset (UTF-16) bookkeeping between them.
#[derive(Default)]
struct Accumulator {
    out: Vec<(Token, i64)>,
    last_pos: i64,
    last_off: usize,
    started: bool,
}

impl Accumulator {
    fn add(&mut self, text: &str, tokens: Vec<Token>, gap: u32) {
        if !self.started {
            self.last_pos = -1;
            self.started = true;
        }
        let chars: Vec<char> = text.chars().collect();
        // Char index -> UTF-16 offset.
        let mut u16_at = Vec::with_capacity(chars.len() + 1);
        let mut n = 0;
        for c in &chars {
            u16_at.push(n);
            n += c.len_utf16();
        }
        u16_at.push(n);
        let at = |c: usize| u16_at.get(c).copied().unwrap_or(n);
        for mut t in tokens {
            if t.pos_inc > 0 {
                self.last_pos += i64::from(t.pos_inc);
            }
            t.start = self.last_off + at(t.start);
            t.end = self.last_off + at(t.end);
            self.out.push((t, self.last_pos.max(0)));
        }
        self.last_off += n;
        self.last_pos += i64::from(gap);
        self.last_off += 1;
    }
}

/// Which extended attributes explain output shows.
struct Attrs {
    keyword: bool,
    only: Option<Vec<String>>,
}

fn token_json(t: &Token, pos: i64, attrs: Option<&Attrs>) -> Value {
    let mut m = Map::new();
    m.insert("token".into(), json!(t.term));
    m.insert("start_offset".into(), json!(t.start));
    m.insert("end_offset".into(), json!(t.end));
    m.insert("type".into(), json!(t.ty));
    m.insert("position".into(), json!(pos));
    match attrs {
        None => {
            if t.pos_len > 1 {
                m.insert("positionLength".into(), json!(t.pos_len));
            }
        }
        Some(a) => {
            let show = |k: &str| a.only.as_ref().is_none_or(|o| o.iter().any(|x| x == k));
            if show("bytes") {
                let hex: Vec<String> =
                    t.term.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
                m.insert("bytes".into(), json!(format!("[{}]", hex.join(" "))));
            }
            if a.keyword && show("keyword") {
                m.insert("keyword".into(), json!(t.keyword));
            }
            if show("positionLength") {
                m.insert("positionLength".into(), json!(t.pos_len));
            }
            if show("termFrequency") {
                m.insert("termFrequency".into(), json!(1));
            }
        }
    }
    Value::Object(m)
}
