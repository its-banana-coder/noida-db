//! Text analysis, as Elasticsearch (Lucene) does it: char filters, a
//! tokenizer and token filters chained into analyzers, built from the
//! built-in names or an index's `analysis` settings, and applied to
//! `text` fields at index and search time (and `keyword` normalizers).
//!
//! An index's analysis settings reach the search code through a
//! per-request scope ([`enter`]), set up by the engine before it runs a
//! request against that index; built analyzers are cached in it.
#![allow(clippy::type_complexity, clippy::enum_variant_names, clippy::collapsible_if, clippy::explicit_counter_loop)]

mod api;
mod backtrack;
mod char_filters;
mod chars;
mod filters;
mod fold;
mod jregex;
mod registry;
mod stemmers;
mod stopwords;
mod synonyms;
mod token;
mod tokenizers;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::{Value, json};

pub use api::{IndexInfo, analyze as analyze_api};
pub use synonyms::validate_api_rule;
pub use token::{Token, positions};

use char_filters::CharFilter;
use filters::TokenFilter;
use registry::Defs;
use tokenizers::Tokenizer;

/// An analysis error as Elasticsearch reports it.
#[derive(Debug, Clone)]
pub struct AnalysisError {
    pub ty: &'static str,
    pub reason: String,
    pub status: u16,
    pub caused_by: Option<Box<AnalysisError>>,
}

/// Exception types that aren't Elasticsearch's own (Java's): the root
/// cause search stops at them.
fn is_plain_java(ty: &str) -> bool {
    matches!(
        ty,
        "illegal_argument_exception"
            | "illegal_state_exception"
            | "pattern_syntax_exception"
            | "number_format_exception"
    )
}

impl AnalysisError {
    pub fn new(ty: &'static str, reason: impl Into<String>) -> Self {
        AnalysisError { ty, reason: reason.into(), status: 400, caused_by: None }
    }

    pub fn with_status(ty: &'static str, reason: impl Into<String>, status: u16) -> Self {
        AnalysisError { ty, reason: reason.into(), status, caused_by: None }
    }

    pub fn caused(mut self, cause: AnalysisError) -> Self {
        self.caused_by = Some(Box::new(cause));
        self
    }

    fn cause_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("type".into(), json!(self.ty));
        m.insert("reason".into(), json!(self.reason));
        if let Some(c) = &self.caused_by {
            m.insert("caused_by".into(), c.cause_json());
        }
        Value::Object(m)
    }

    /// Elasticsearch's root cause: the error itself unless it is one of
    /// its own exceptions, then the innermost of its own causes.
    fn root(&self) -> &AnalysisError {
        let mut r = self;
        if is_plain_java(r.ty) {
            return r;
        }
        while let Some(c) = &r.caused_by {
            if is_plain_java(c.ty) {
                break;
            }
            r = c;
        }
        r
    }

    /// The `{"error": ..., "status": ...}` body.
    pub fn to_json(&self) -> Value {
        let root = self.root();
        let mut m = serde_json::Map::new();
        m.insert("root_cause".into(), json!([{"type": root.ty, "reason": root.reason}]));
        m.insert("type".into(), json!(self.ty));
        m.insert("reason".into(), json!(self.reason));
        if let Some(c) = &self.caused_by {
            m.insert("caused_by".into(), c.cause_json());
        }
        json!({"error": Value::Object(m), "status": self.status})
    }
}

/// A complete analyzer (or normalizer: keyword tokenizer).
#[derive(Debug, Clone)]
pub struct Analyzer {
    pub name: String,
    /// Shown stage by stage in `_analyze` explain output.
    pub custom: bool,
    pub char_filters: Vec<(String, CharFilter)>,
    pub tokenizer: (String, Tokenizer),
    pub filters: Vec<(String, TokenFilter)>,
    pub position_increment_gap: u32,
}

/// Every stage of an analysis, for `_analyze` explain.
pub struct Stages {
    /// Each char filter's output text.
    pub char_filters: Vec<String>,
    pub tokenizer: Vec<Token>,
    /// The tokens after each filter.
    pub filters: Vec<Vec<Token>>,
}

impl Analyzer {
    /// The tokens of one value, offsets as character indices into it.
    pub fn tokens(&self, text: &str) -> Vec<Token> {
        let (toks, _) = self.run(text, false);
        toks
    }

    pub fn stages(&self, text: &str) -> Stages {
        self.run(text, true).1.unwrap_or(Stages {
            char_filters: vec![],
            tokenizer: vec![],
            filters: vec![],
        })
    }

    fn run(&self, text: &str, keep: bool) -> (Vec<Token>, Option<Stages>) {
        let mut cur: Vec<char> = text.chars().collect();
        let mut corrections = Vec::new();
        let mut cf_out = Vec::new();
        for (_, cf) in &self.char_filters {
            let (out, corr) = cf.apply(&cur);
            cur = out;
            corrections.push(corr);
            if keep {
                cf_out.push(cur.iter().collect());
            }
        }
        let mut toks = self.tokenizer.1.tokenize(&cur);
        if !corrections.is_empty() {
            for t in &mut toks {
                for c in corrections.iter().rev() {
                    t.start = c.correct(t.start);
                    t.end = c.correct(t.end);
                }
            }
        }
        if !keep {
            for (_, f) in &self.filters {
                toks = f.apply(toks);
            }
            return (toks, None);
        }
        let mut stages = Vec::new();
        let mut last = toks.clone();
        for (_, f) in &self.filters {
            last = f.apply(last);
            stages.push(last.clone());
        }
        (last, Some(Stages { char_filters: cf_out, tokenizer: toks, filters: stages }))
    }

    pub fn terms(&self, text: &str) -> Vec<String> {
        self.tokens(text).into_iter().map(|t| t.term).collect()
    }
}

// ---------------------------------------------------------------------
// The per-request scope: the target index's analysis settings.

/// What the engine provides for a request: the analysis settings (and
/// limits) of the index (or indices) it targets, and the synonym sets
/// they use.
#[derive(Default)]
pub struct Scope {
    pub analysis: Value,
    pub index_settings: Value,
    pub synonym_sets: HashMap<String, Vec<String>>,
}

struct ScopeState {
    scope: Scope,
    cache: RefCell<HashMap<String, Result<Rc<Analyzer>, AnalysisError>>>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<ScopeState>>> = const { RefCell::new(None) };
    /// Built-in analyzers, built once per thread.
    static BUILTIN: RefCell<HashMap<String, Option<Rc<Analyzer>>>> = RefCell::new(HashMap::new());
}

/// Restores the previous scope when dropped.
pub struct ScopeGuard(Option<Rc<ScopeState>>);

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        let prev = self.0.take();
        CURRENT.with(|c| *c.borrow_mut() = prev);
    }
}

/// Makes `scope` the analysis scope of this thread until the guard drops.
pub fn enter(scope: Scope) -> ScopeGuard {
    let state = Rc::new(ScopeState { scope, cache: RefCell::new(HashMap::new()) });
    let prev = CURRENT.with(|c| c.borrow_mut().replace(state));
    ScopeGuard(prev)
}

fn with_defs<R>(f: impl FnOnce(&Defs, Option<&ScopeState>) -> R) -> R {
    let state = CURRENT.with(|c| c.borrow().clone());
    let null = Value::Null;
    match &state {
        Some(s) => {
            let sets = |set: &str| s.scope.synonym_sets.get(set).cloned();
            let defs = Defs {
                analysis: &s.scope.analysis,
                index_settings: &s.scope.index_settings,
                synonym_sets: &sets,
            };
            f(&defs, Some(s))
        }
        None => {
            let defs = Defs { analysis: &null, index_settings: &null, synonym_sets: &|_| None };
            f(&defs, None)
        }
    }
}

fn builtin(name: &str) -> Option<Rc<Analyzer>> {
    BUILTIN.with(|b| {
        b.borrow_mut()
            .entry(name.to_string())
            .or_insert_with(|| registry::builtin_analyzer(name).map(Rc::new))
            .clone()
    })
}

/// An analyzer (or, with `normalizer`, a normalizer) by name in the
/// current scope; `None` when no such name exists.
fn lookup(name: &str, normalizer: bool) -> Option<Result<Rc<Analyzer>, AnalysisError>> {
    with_defs(|defs, state| {
        let section = if normalizer { "normalizer" } else { "analyzer" };
        let defined = defs.analysis.get(section).and_then(|s| s.get(name)).is_some();
        if !defined && !normalizer {
            return builtin(name).map(Ok);
        }
        let key = format!("{section}:{name}");
        if let Some(st) = state
            && let Some(hit) = st.cache.borrow().get(&key)
        {
            return Some(hit.clone());
        }
        let built =
            if normalizer { defs.normalizer_named(name) } else { defs.analyzer_named(name) };
        let built = built.map(|r| r.map(Rc::new));
        if let (Some(st), Some(b)) = (state, &built) {
            st.cache.borrow_mut().insert(key, b.clone());
        }
        built
    })
}

/// The analyzer named `name` (index-defined or built-in), falling back
/// to `standard` when it can't be built.
pub fn analyzer(name: &str) -> Rc<Analyzer> {
    match lookup(name, false) {
        Some(Ok(a)) => a,
        _ => builtin("standard").expect("standard analyzer"),
    }
}

/// Whether an analyzer of that name exists in the current scope.
pub fn analyzer_exists(name: &str) -> bool {
    lookup(name, false).is_some()
}

// ---------------------------------------------------------------------
// Fields: which analyzer a mapped field uses.

/// Which of a field's analyzers.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Mode {
    Index,
    Search,
    Quote,
}

/// A field's definition in `mappings` (multi-fields included).
pub fn field_def<'a>(mappings: &'a Value, field: &str) -> Option<&'a Value> {
    let segs: Vec<&str> = field.split('.').collect();
    let mut props = mappings.get("properties");
    for (i, seg) in segs.iter().enumerate() {
        let node = props?.get(*seg)?;
        if i + 1 == segs.len() {
            return Some(node);
        }
        if i + 2 == segs.len()
            && let Some(sub) = node.get("fields").and_then(|f| f.get(segs[i + 1]))
        {
            return Some(sub);
        }
        props = node.get("properties");
    }
    None
}

/// `default`, `default_search` or `default_search_quoted` when the index
/// defines it.
fn index_default(kind: &str) -> Option<String> {
    with_defs(|defs, _| {
        defs.analysis.get("analyzer").and_then(|s| s.get(kind)).map(|_| kind.to_string())
    })
}

/// The name of the analyzer a text field uses in `mode`.
pub fn field_analyzer_name(mappings: &Value, field: &str, mode: Mode) -> String {
    let def = field_def(mappings, field);
    let get = |k: &str| def.and_then(|d| d.get(k)).and_then(Value::as_str).map(str::to_string);
    let index = || {
        get("analyzer").or_else(|| index_default("default")).unwrap_or_else(|| "standard".into())
    };
    let search = || {
        get("search_analyzer")
            .or_else(|| get("analyzer"))
            .or_else(|| index_default("default_search"))
            .unwrap_or_else(index)
    };
    match mode {
        Mode::Index => index(),
        Mode::Search => search(),
        Mode::Quote => get("search_quote_analyzer")
            .or_else(|| {
                if get("search_analyzer").is_none() && get("analyzer").is_none() {
                    index_default("default_search_quoted")
                } else {
                    None
                }
            })
            .unwrap_or_else(search),
    }
}

/// The analyzer a text field uses in `mode`.
pub fn field_analyzer(mappings: &Value, field: &str, mode: Mode) -> Rc<Analyzer> {
    analyzer(&field_analyzer_name(mappings, field, mode))
}

/// Terms of `text` as the text field `field` analyzes it in `mode`.
pub fn field_terms(mappings: &Value, field: &str, text: &str, mode: Mode) -> Vec<String> {
    field_analyzer(mappings, field, mode).terms(text)
}

/// Tokens of `text` for the text field `field` in `mode`.
pub fn field_tokens(mappings: &Value, field: &str, text: &str, mode: Mode) -> Vec<Token> {
    field_analyzer(mappings, field, mode).tokens(text)
}

/// The terms of a field's values with their absolute positions (each
/// value after the previous one's last position plus the field's
/// `position_increment_gap`), for phrase matching.
pub fn field_positions(
    mappings: &Value,
    field: &str,
    values: &[&str],
    mode: Mode,
) -> Vec<(String, i64)> {
    let a = field_analyzer(mappings, field, mode);
    let gap = field_def(mappings, field)
        .and_then(|d| d.get("position_increment_gap"))
        .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(100);
    let mut out = Vec::new();
    let mut base = -1i64;
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            base += gap;
        }
        let mut pos = base;
        for t in a.tokens(v) {
            pos += i64::from(t.pos_inc);
            out.push((t.term, pos.max(0)));
        }
        base = pos.max(base);
    }
    out
}

/// The phrase frequency of `query` in `doc`: the number of exact matches
/// (`slop` 0), or the sum of Lucene's sloppy frequency `1 / (1 + distance)`
/// over the matches within `slop`.
pub fn phrase_freq(doc: &[(String, i64)], query: &[(String, i64)], slop: usize) -> f32 {
    if query.is_empty() {
        return 0.0;
    }
    let mut groups: Vec<(i64, Vec<&str>)> = Vec::new();
    for (t, p) in query {
        match groups.iter_mut().find(|g| g.0 == *p) {
            Some(g) => g.1.push(t),
            None => groups.push((*p, vec![t])),
        }
    }
    groups.sort_by_key(|g| g.0);
    let cands: Vec<Vec<i64>> = groups
        .iter()
        .map(|(_, terms)| {
            let mut ps: Vec<i64> =
                doc.iter().filter(|(t, _)| terms.contains(&t.as_str())).map(|(_, p)| *p).collect();
            ps.sort();
            ps.dedup();
            ps
        })
        .collect();
    if cands.iter().any(Vec::is_empty) {
        return 0.0;
    }
    let base = groups[0].0;
    if slop == 0 {
        return cands[0]
            .iter()
            .filter(|&&start| {
                groups.iter().zip(&cands).all(|((qp, _), ps)| ps.binary_search(&(start + qp - base)).is_ok())
            })
            .count() as f32;
    }
    // Each position of the first term anchors one match: its smallest
    // spread of (document - query) position offsets.
    fn best(groups: &[(i64, Vec<&str>)], cands: &[Vec<i64>], k: usize, chosen: &mut Vec<i64>) -> Option<i64> {
        if k == groups.len() {
            let offs: Vec<i64> = chosen.iter().zip(groups).map(|(p, g)| p - g.0).collect();
            let lo = offs.iter().min().copied().unwrap_or(0);
            let hi = offs.iter().max().copied().unwrap_or(0);
            return Some(hi - lo);
        }
        let mut out: Option<i64> = None;
        for &p in &cands[k] {
            if chosen.contains(&p) {
                continue;
            }
            chosen.push(p);
            if let Some(d) = best(groups, cands, k + 1, chosen) {
                out = Some(out.map_or(d, |o| o.min(d)));
            }
            chosen.pop();
        }
        out
    }
    let mut freq = 0.0;
    for &anchor in &cands[0] {
        let mut chosen = vec![anchor];
        if let Some(d) = best(&groups, &cands, 1, &mut chosen)
            && d <= slop as i64
        {
            freq += 1.0 / (1.0 + d as f32);
        }
    }
    freq
}

/// Whether the phrase `query` (terms at relative positions; terms at the
/// same position are alternatives) occurs in `doc` with at most `slop`
/// total displacement (Lucene's phrase / sloppy phrase match).
pub fn phrase_matches(doc: &[(String, i64)], query: &[(String, i64)], slop: usize) -> bool {
    if query.is_empty() {
        return false;
    }
    // Query positions, each with its alternative terms.
    let mut groups: Vec<(i64, Vec<&str>)> = Vec::new();
    for (t, p) in query {
        match groups.iter_mut().find(|g| g.0 == *p) {
            Some(g) => g.1.push(t),
            None => groups.push((*p, vec![t])),
        }
    }
    groups.sort_by_key(|g| g.0);
    let base = groups[0].0;
    let cands: Vec<Vec<i64>> = groups
        .iter()
        .map(|(_, terms)| {
            let mut ps: Vec<i64> =
                doc.iter().filter(|(t, _)| terms.contains(&t.as_str())).map(|(_, p)| *p).collect();
            ps.sort();
            ps.dedup();
            ps
        })
        .collect();
    if cands.iter().any(Vec::is_empty) {
        return false;
    }
    if slop == 0 {
        return cands[0].iter().any(|&start| {
            groups
                .iter()
                .zip(&cands)
                .all(|((qp, _), ps)| ps.binary_search(&(start + qp - base)).is_ok())
        });
    }
    fn search(
        groups: &[(i64, Vec<&str>)],
        cands: &[Vec<i64>],
        k: usize,
        chosen: &mut Vec<i64>,
        slop: i64,
    ) -> bool {
        if k == groups.len() {
            let offs: Vec<i64> = chosen.iter().zip(groups).map(|(p, g)| p - g.0).collect();
            let (lo, hi) =
                (offs.iter().min().copied().unwrap_or(0), offs.iter().max().copied().unwrap_or(0));
            return hi - lo <= slop;
        }
        for &p in &cands[k] {
            if chosen.contains(&p) {
                continue;
            }
            chosen.push(p);
            if search(groups, cands, k + 1, chosen, slop) {
                return true;
            }
            chosen.pop();
        }
        false
    }
    search(&groups, &cands, 0, &mut Vec::new(), slop as i64)
}

/// A keyword field's value after its `normalizer` (unchanged without
/// one).
pub fn normalize(mappings: &Value, field: &str, value: &str) -> String {
    let Some(name) =
        field_def(mappings, field).and_then(|d| d.get("normalizer")).and_then(Value::as_str)
    else {
        return value.to_string();
    };
    match lookup(name, true) {
        Some(Ok(n)) => n.tokens(value).into_iter().map(|t| t.term).collect::<Vec<_>>().join(""),
        _ => value.to_string(),
    }
}

// ---------------------------------------------------------------------
// Index settings and mappings checks.

/// Checks an index's analysis settings (every component builds) and the
/// analyzers / normalizers its mappings name, as index creation and
/// mapping updates do. `synonym_sets` resolves `synonyms_set` names.
pub fn validate_index(
    settings: &Value,
    mappings: &Value,
    synonym_sets: &dyn Fn(&str) -> Option<Vec<String>>,
) -> Result<(), AnalysisError> {
    let analysis = settings.get("index").and_then(|i| i.get("analysis")).unwrap_or(&Value::Null);
    let index_settings = settings.get("index").unwrap_or(&Value::Null);
    // A missing synonym set doesn't fail creation (the shards fail to
    // allocate instead): check with it empty.
    let lenient_sets = |s: &str| Some(synonym_sets(s).unwrap_or_default());
    let defs = Defs { analysis, index_settings, synonym_sets: &lenient_sets };
    defs.validate()?;
    let mut err = None;
    walk_fields(mappings.get("properties"), "", &mut |path, def| {
        if err.is_none() {
            err = check_field(&defs, analysis, path, def).err();
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn check_field(
    defs: &Defs,
    analysis: &Value,
    path: &str,
    def: &Value,
) -> Result<(), AnalysisError> {
    let ty = def.get("type").and_then(Value::as_str).unwrap_or("object");
    let text_like = matches!(ty, "text" | "search_as_you_type" | "completion" | "annotated_text");
    for key in ["analyzer", "search_analyzer", "search_quote_analyzer"] {
        let Some(name) = def.get(key).and_then(Value::as_str) else { continue };
        if !text_like {
            continue;
        }
        let exists = analysis.get("analyzer").and_then(|s| s.get(name)).is_some()
            || registry::builtin_analyzer(name).is_some();
        if !exists {
            let reason = format!("analyzer [{name}] has not been configured in mappings");
            return Err(AnalysisError::new(
                "mapper_parsing_exception",
                format!("Failed to parse mapping: {reason}"),
            )
            .caused(AnalysisError::new("illegal_argument_exception", reason)));
        }
        if key == "analyzer" && ty == "text" {
            let bad = defs.updateable_filters(name);
            if !bad.is_empty() {
                let reason = format!(
                    "analyzer [{name}] contains filters [{}] that are not allowed to run in index time mode.",
                    bad.join(", ")
                );
                return Err(AnalysisError::new(
                    "mapper_parsing_exception",
                    format!("Failed to parse mapping: {reason}"),
                )
                .caused(AnalysisError::new("mapper_exception", reason)));
            }
        }
    }
    if ty == "keyword"
        && let Some(name) = def.get("normalizer").and_then(Value::as_str)
    {
        let exists =
            analysis.get("normalizer").and_then(|s| s.get(name)).is_some() || name == "lowercase";
        if !exists {
            let reason = format!(
                "normalizer [{name}] not found for field [{}]",
                path.rsplit('.').next().unwrap_or(path)
            );
            return Err(AnalysisError::new(
                "mapper_parsing_exception",
                format!("Failed to parse mapping: {reason}"),
            )
            .caused(AnalysisError::new("mapper_parsing_exception", reason)));
        }
    }
    Ok(())
}

fn walk_fields(props: Option<&Value>, prefix: &str, f: &mut dyn FnMut(&str, &Value)) {
    let Some(p) = props.and_then(Value::as_object) else { return };
    for (k, def) in p {
        let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        f(&path, def);
        if let Some(fields) = def.get("fields").and_then(Value::as_object) {
            for (sk, sd) in fields {
                f(&format!("{path}.{sk}"), sd);
            }
        }
        walk_fields(def.get("properties"), &path, f);
    }
}

/// For each synonym set an index's analysis settings use, the analyzers
/// that reload when it changes.
pub fn synonym_set_users(settings: &Value) -> HashMap<String, Vec<String>> {
    let analysis = settings.get("index").and_then(|i| i.get("analysis")).unwrap_or(&Value::Null);
    let defs = Defs { analysis, index_settings: &Value::Null, synonym_sets: &|_| None };
    defs.synonym_set_users()
}

/// Every synonym set an index's filters name.
pub fn synonym_sets_used(settings: &Value) -> Vec<String> {
    let analysis = settings.get("index").and_then(|i| i.get("analysis")).unwrap_or(&Value::Null);
    let defs = Defs { analysis, index_settings: &Value::Null, synonym_sets: &|_| None };
    defs.synonym_sets_used()
}

/// Tokens as (term, byte start, byte end) in `text`.
pub fn with_byte_offsets(text: &str, tokens: Vec<Token>) -> Vec<(String, usize, usize)> {
    let mut idx: Vec<usize> = text.char_indices().map(|(b, _)| b).collect();
    idx.push(text.len());
    let at = |c: usize| idx.get(c).copied().unwrap_or(text.len());
    tokens.into_iter().map(|t| (t.term, at(t.start), at(t.end))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyze(name: &str, text: &str) -> Vec<String> {
        analyzer(name).terms(text)
    }

    #[test]
    fn standard_lowercases_and_splits_on_punctuation() {
        assert_eq!(
            analyze("standard", "The Quick-Brown Fox!"),
            vec!["the", "quick", "brown", "fox"]
        );
    }

    #[test]
    fn standard_keeps_apostrophes_and_decimals_inside_tokens() {
        assert_eq!(
            analyze("standard", "don't split 3.14 please"),
            vec!["don't", "split", "3.14", "please"]
        );
    }

    #[test]
    fn whitespace_preserves_case_and_punctuation() {
        assert_eq!(analyze("whitespace", "Hello, World!"), vec!["Hello,", "World!"]);
    }

    #[test]
    fn keyword_is_a_single_token() {
        assert_eq!(analyze("keyword", "New York"), vec!["New York"]);
    }

    #[test]
    fn stop_removes_common_words() {
        assert_eq!(analyze("stop", "the quick fox and the dog"), vec!["quick", "fox", "dog"]);
    }

    #[test]
    fn english_analyzer() {
        assert_eq!(
            analyze("english", "The dancing stars were shining; John's cats' toys"),
            vec!["danc", "star", "were", "shine", "john", "cat", "toi"]
        );
    }

    #[test]
    fn scoped_custom_analyzer_and_field_resolution() {
        let scope = Scope {
            analysis: json!({
                "analyzer": {"my": {"tokenizer": "whitespace", "filter": ["lowercase", "my_edge"]}},
                "filter": {"my_edge": {"type": "edge_ngram", "min_gram": "2", "max_gram": "3"}},
                "normalizer": {"n": {"type": "custom", "filter": ["lowercase", "asciifolding"]}}
            }),
            ..Default::default()
        };
        let _g = enter(scope);
        let mappings = json!({"properties": {
            "t": {"type": "text", "analyzer": "my", "search_analyzer": "standard"},
            "k": {"type": "keyword", "normalizer": "n"}
        }});
        assert_eq!(field_terms(&mappings, "t", "Dance", Mode::Index), ["da", "dan"]);
        assert_eq!(field_terms(&mappings, "t", "Dance", Mode::Search), ["dance"]);
        assert_eq!(normalize(&mappings, "k", "Héllo"), "hello");
    }

    #[test]
    fn index_validation_errors() {
        let settings = json!({"index": {"analysis": {"analyzer": {"a": {"type": "custom", "tokenizer": "nope"}}}}});
        let e = validate_index(&settings, &json!({}), &|_| None).unwrap_err();
        assert_eq!(e.reason, "Custom Analyzer [a] failed to find tokenizer under name [nope]");
        let e = validate_index(
            &json!({}),
            &json!({"properties": {"t": {"type": "text", "analyzer": "x"}}}),
            &|_| None,
        )
        .unwrap_err();
        assert_eq!(e.ty, "mapper_parsing_exception");
        assert_eq!(e.to_json()["error"]["root_cause"][0]["type"], "mapper_parsing_exception");
    }
}
