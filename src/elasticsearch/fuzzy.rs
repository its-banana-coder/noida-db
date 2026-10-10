//! Fuzzy matching and the full-text options of the `match` family:
//! `fuzziness` (`AUTO`, `AUTO:low,high`, 0-2), `prefix_length`,
//! `max_expansions`, `fuzzy_transpositions`, `fuzzy_rewrite`, and
//! `minimum_should_match`, for `fuzzy`, `match`, `match_bool_prefix` and
//! `multi_match` (`best_fields`, `most_fields`, `bool_prefix`).
//!
//! A fuzzy term expands to the field's index terms within the allowed edit
//! distance (the best `max_expansions` of them), each scored as a BM25 term
//! with the document frequency blended to the highest among them and
//! boosted by its similarity, `1 - edits / min(len)` -- Lucene's
//! `FuzzyQuery` with its default `top_terms_blended_freqs` rewrite.

use std::collections::HashMap;

use serde_json::{Map, Value};

use super::analysis;
use super::search::{
    CommittedDoc, EsError, analyze_for, doc_tokens, eval, field_and_spec, query_text, resolve_field,
};
use super::{highlight, scoring};

type Scores = HashMap<usize, f32>;

/// A parsed `fuzziness`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Fuzziness {
    /// `AUTO:low,high`: no edits below `low` characters, one below `high`,
    /// two from there on (`AUTO` is `AUTO:3,6`).
    Auto(usize, usize),
    Edits(usize),
}

impl Fuzziness {
    pub(crate) fn edits(self, term: &str) -> usize {
        match self {
            Fuzziness::Edits(n) => n,
            Fuzziness::Auto(low, high) => {
                let len = term.chars().count();
                if len < low {
                    0
                } else if len < high {
                    1
                } else {
                    2
                }
            }
        }
    }
}

fn bad_fuzziness(shown: &str) -> EsError {
    EsError::new(400, "illegal_argument_exception", &format!("fuzziness cannot be [{shown}]."))
        .caused_by(
            "number_format_exception",
            &format!("For input string: \"{}\"", shown.to_uppercase()),
        )
}

fn edits_from(n: f64) -> Result<Fuzziness, EsError> {
    let n = n.floor();
    if !(0.0..=2.0).contains(&n) {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("Valid edit distances are [0, 1, 2] but was [{n}]"),
        ));
    }
    Ok(Fuzziness::Edits(n as usize))
}

/// Parses a `fuzziness` value the way Elasticsearch's `Fuzziness` does.
pub(crate) fn parse_fuzziness(v: &Value) -> Result<Fuzziness, EsError> {
    match v {
        Value::Number(n) => edits_from(n.as_f64().unwrap_or(0.0)),
        Value::String(s) => {
            let upper = s.trim().to_uppercase();
            if upper == "AUTO" {
                return Ok(Fuzziness::Auto(3, 6));
            }
            if let Some(rest) = upper.strip_prefix("AUTO:") {
                let bounds: Vec<Option<usize>> =
                    rest.split(',').map(|p| p.trim().parse::<usize>().ok()).collect();
                return match bounds.as_slice() {
                    [Some(low), Some(high)] if low <= high => Ok(Fuzziness::Auto(*low, *high)),
                    _ => Err(EsError::parsing(&format!(
                        "failed to find low and high distance values within the fuzziness \
                         value [{s}]"
                    ))),
                };
            }
            match upper.parse::<f64>() {
                Ok(n) => edits_from(n),
                Err(_) => Err(bad_fuzziness(s)),
            }
        }
        other => Err(bad_fuzziness(&other.to_string())),
    }
}

/// Levenshtein distance, counting an adjacent transposition as one edit
/// when `transpositions` is set (Damerau, optimal string alignment).
pub(crate) fn edit_distance(a: &[char], b: &[char], transpositions: bool) -> usize {
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1).min(d[i][j - 1] + 1).min(d[i - 1][j - 1] + cost);
            if transpositions && i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[a.len()][b.len()]
}

/// The fuzzy options shared by `fuzzy` and the `match` family.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FuzzyOpts {
    pub fuzziness: Fuzziness,
    pub prefix_length: usize,
    pub max_expansions: usize,
    pub transpositions: bool,
    /// `rewrite`/`fuzzy_rewrite: constant_score`: every match scores 1.
    pub constant: bool,
}

fn usize_opt(o: &Map<String, Value>, key: &str, default: usize) -> usize {
    match o.get(key) {
        Some(Value::Number(n)) => n.as_u64().unwrap_or(default as u64) as usize,
        Some(Value::String(s)) => s.parse().unwrap_or(default),
        _ => default,
    }
}

fn bool_opt(o: &Map<String, Value>, key: &str, default: bool) -> bool {
    match o.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => default,
    }
}

impl FuzzyOpts {
    /// The fuzzy options of a `match`-family spec (`None` without
    /// `fuzziness`).
    pub(crate) fn of_match(o: &Map<String, Value>) -> Result<Option<Self>, EsError> {
        let Some(f) = o.get("fuzziness") else { return Ok(None) };
        Ok(Some(FuzzyOpts {
            fuzziness: parse_fuzziness(f)?,
            prefix_length: usize_opt(o, "prefix_length", 0),
            max_expansions: usize_opt(o, "max_expansions", 50),
            transpositions: bool_opt(o, "fuzzy_transpositions", true),
            constant: is_constant_rewrite(o.get("fuzzy_rewrite")),
        }))
    }
}

fn is_constant_rewrite(v: Option<&Value>) -> bool {
    matches!(v.and_then(Value::as_str), Some("constant_score" | "constant_score_boolean"))
}

/// One field's analyzed terms per document, with BM25's collection
/// statistics.
pub(crate) struct FieldTerms {
    pub toks: Vec<Vec<String>>,
    doc_count: u64,
    avg_len: f32,
}

impl FieldTerms {
    pub(crate) fn new(mappings: &Value, docs: &[CommittedDoc], field: &str) -> Self {
        let toks = doc_tokens(mappings, docs, field);
        let doc_count = toks.iter().filter(|t| !t.is_empty()).count() as u64;
        let total: u64 = toks.iter().map(|t| t.len() as u64).sum();
        let avg_len = if doc_count > 0 { total as f32 / doc_count as f32 } else { 1.0 };
        FieldTerms { toks, doc_count, avg_len }
    }

    pub(crate) fn doc_freq(&self, term: &str) -> u64 {
        self.toks.iter().filter(|t| t.iter().any(|x| x == term)).count() as u64
    }

    /// BM25 of `term` in each document containing it, with `df` as its
    /// document frequency.
    pub(crate) fn term_scores(&self, term: &str, df: u64, boost: f32) -> Scores {
        let mut out = Scores::new();
        for (i, toks) in self.toks.iter().enumerate() {
            let tf = toks.iter().filter(|t| *t == term).count() as u32;
            if tf == 0 {
                continue;
            }
            let len = scoring::norm_doc_len(toks.len() as u32).max(1);
            out.insert(i, boost * scoring::score(tf, len, self.avg_len, df, self.doc_count.max(1)));
        }
        out
    }

    /// BM25 of `term` on a field without norms (keyword, boolean): every
    /// document's length counts as 1 against the average number of values.
    pub(crate) fn term_scores_no_norms(&self, term: &str, boost: f32) -> Scores {
        let total: u64 = self.toks.iter().map(|t| t.len() as u64).sum();
        let avg = if self.doc_count > 0 { total as f32 / self.doc_count as f32 } else { 1.0 };
        let idf = scoring::idf(self.doc_freq(term), self.doc_count.max(1));
        let norm = scoring::K1 * ((1.0 - scoring::B) + scoring::B / avg);
        let mut out = Scores::new();
        for (i, toks) in self.toks.iter().enumerate() {
            let tf = toks.iter().filter(|t| *t == term).count() as f32;
            if tf > 0.0 {
                out.insert(i, boost * idf * tf * (scoring::K1 + 1.0) / (tf + norm));
            }
        }
        out
    }

    /// Every distinct index term of the field, sorted.
    pub(crate) fn terms(&self) -> Vec<&String> {
        let mut all: Vec<&String> = self.toks.iter().flatten().collect();
        all.sort();
        all.dedup();
        all
    }

    /// The index terms within `opts`' edit distance of `term`, each with
    /// its similarity boost: the best `max_expansions`, highest boost
    /// first (ties in term order).
    pub(crate) fn fuzzy_expansions(&self, term: &str, opts: &FuzzyOpts) -> Vec<(String, f32)> {
        let q: Vec<char> = term.chars().collect();
        let max_edits = opts.fuzziness.edits(term);
        let prefix_len = opts.prefix_length.min(q.len());
        let mut out: Vec<(String, f32)> = Vec::new();
        for t in self.terms() {
            let c: Vec<char> = t.chars().collect();
            if c.len() < prefix_len || c[..prefix_len] != q[..prefix_len] {
                continue;
            }
            if c.len().abs_diff(q.len()) > max_edits {
                continue;
            }
            let ed = edit_distance(&c[prefix_len..], &q[prefix_len..], opts.transpositions);
            if ed > max_edits {
                continue;
            }
            let boost =
                if ed == 0 { 1.0 } else { 1.0 - ed as f32 / c.len().min(q.len()).max(1) as f32 };
            out.push((t.clone(), boost));
        }
        out.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out.truncate(opts.max_expansions.max(1));
        out
    }

    /// A fuzzy term's scores: its expansions as SHOULD clauses with the
    /// blended document frequency.
    pub(crate) fn fuzzy_scores(&self, term: &str, opts: &FuzzyOpts, boost: f32) -> Scores {
        let exps = self.fuzzy_expansions(term, opts);
        let df = exps.iter().map(|(t, _)| self.doc_freq(t)).max().unwrap_or(0);
        let mut out = Scores::new();
        for (t, b) in exps {
            let scores = if opts.constant {
                self.toks
                    .iter()
                    .enumerate()
                    .filter(|(_, toks)| toks.contains(&t))
                    .map(|(i, _)| (i, boost))
                    .collect()
            } else {
                self.term_scores(&t, df, b * boost)
            };
            for (i, s) in scores {
                let e = out.entry(i).or_insert(0.0);
                *e = if opts.constant { boost } else { *e + s };
            }
        }
        out
    }

    /// The constant-score `prefix` clause of `match_bool_prefix`.
    fn prefix_scores(&self, prefix: &str, boost: f32) -> Scores {
        self.toks
            .iter()
            .enumerate()
            .filter(|(_, toks)| toks.iter().any(|t| t.starts_with(prefix)))
            .map(|(i, _)| (i, boost))
            .collect()
    }
}

/// `minimum_should_match` for `optional` SHOULD clauses: an integer
/// (negative: that many may be missing), a percentage (`75%`, `-25%`) or
/// conditional specs (`3<90%`, `2<-25% 9<-3`) -- Elasticsearch's
/// `Queries.calculateMinShouldMatch`.
pub(crate) fn min_should_match(spec: &Value, optional: usize) -> Result<usize, EsError> {
    let text = match spec {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => return Ok(0),
    };
    let bad = || {
        EsError::new(
            400,
            "illegal_argument_exception",
            &format!("Invalid minimum_should_match value [{text}]"),
        )
    };
    fn simple(n: i64, spec: &str) -> Option<i64> {
        let spec = spec.trim();
        let result = if let Some(p) = spec.strip_suffix('%') {
            let percent: i64 = p.trim().parse().ok()?;
            let calc = (n * percent) as f32 / 100.0;
            if calc < 0.0 { n + calc as i64 } else { calc as i64 }
        } else {
            let calc: i64 = spec.parse().ok()?;
            if calc < 0 { n + calc } else { calc }
        };
        Some(result.max(0))
    }
    let n = optional as i64;
    let trimmed = text.trim();
    if trimmed.contains('<') {
        let mut result = n;
        for part in trimmed.split_whitespace() {
            let (bound, rest) = part.split_once('<').ok_or_else(bad)?;
            let bound: i64 = bound.trim().parse().map_err(|_| bad())?;
            if n <= bound {
                return Ok(result.max(0) as usize);
            }
            result = simple(n, rest).ok_or_else(bad)?;
        }
        return Ok(result.max(0) as usize);
    }
    simple(n, trimmed).map(|r| r as usize).ok_or_else(bad)
}

/// Combines per-clause scores as a `bool` of SHOULD clauses with
/// `minimum_should_match` (or all required for `and`).
fn combine(clauses: Vec<Scores>, and: bool, msm: Option<&Value>) -> Result<Scores, EsError> {
    let n = clauses.len();
    let required = if and {
        n
    } else {
        match msm {
            Some(spec) if n > 1 => min_should_match(spec, n)?,
            _ => 1,
        }
    };
    let mut out = Scores::new();
    let mut hits: HashMap<usize, usize> = HashMap::new();
    for c in clauses {
        for (i, s) in c {
            *out.entry(i).or_insert(0.0) += s;
            *hits.entry(i).or_insert(0) += 1;
        }
    }
    out.retain(|i, _| hits.get(i).copied().unwrap_or(0) >= required.max(1));
    Ok(out)
}

fn is_and(o: &Map<String, Value>) -> bool {
    o.get("operator").and_then(Value::as_str).is_some_and(|op| op.eq_ignore_ascii_case("and"))
}

fn boost_of(o: &Map<String, Value>) -> f32 {
    o.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32
}

/// The query text analyzed with the request's `analyzer`, or the field's.
fn analyzed(o: &Map<String, Value>, mappings: &Value, field: &str, text: &str) -> Vec<String> {
    match o.get("analyzer").and_then(Value::as_str) {
        Some(a) => analysis::analyze(a, text),
        None => analyze_for(mappings, field, text),
    }
}

/// Fuzzy queries only apply to keyword and text fields.
fn check_fuzzy_mapping(mappings: &Value, field: &str) -> Result<(), EsError> {
    match resolve_field(mappings, field).1 {
        Some(t) => check_fuzzy_field(&t, field),
        None => Ok(()),
    }
}

/// The error for a fuzzy query on a field of type `ty`, if not allowed.
pub(crate) fn check_fuzzy_field(ty: &str, field: &str) -> Result<(), EsError> {
    match ty {
        "text" | "keyword" | "match_only_text" | "constant_keyword" | "wildcard" | "flattened" => {
            Ok(())
        }
        t => {
            let reason = format!(
                "Can only use fuzzy queries on keyword and text fields - not on [{field}] which \
                 is of type [{t}]"
            );
            Err(EsError::shard_failure(
                "query_shard_exception",
                &format!("failed to create query: {reason}"),
            )
            .caused_by("illegal_argument_exception", &reason))
        }
    }
}

/// `match` with any of `fuzziness`, `minimum_should_match` or
/// `zero_terms_query` (the plain form is `search::eval_match`).
pub fn eval_match(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let Some((field, spec)) = field_and_spec(v) else { return Ok(Scores::new()) };
    let Some(o) = spec.as_object() else { return Ok(Scores::new()) };
    let text = query_text(o.get("query"));
    let fuzzy = FuzzyOpts::of_match(o)?;
    if fuzzy.is_some() {
        check_fuzzy_mapping(mappings, field)?;
    }
    let terms = analyzed(o, mappings, field, &text);
    if terms.is_empty() {
        let all = o.get("zero_terms_query").and_then(Value::as_str) == Some("all");
        return Ok(if all { (0..docs.len()).map(|i| (i, 1.0)).collect() } else { Scores::new() });
    }
    let ft = FieldTerms::new(mappings, docs, field);
    let boost = boost_of(o);
    let clauses: Vec<Scores> = terms
        .iter()
        .map(|t| match &fuzzy {
            Some(f) => ft.fuzzy_scores(t, f, boost),
            None => ft.term_scores(t, ft.doc_freq(t), boost),
        })
        .collect();
    combine(clauses, is_and(o), o.get("minimum_should_match"))
}

/// `fuzzy`: the field's terms within the edit distance (default `AUTO`).
pub fn eval_fuzzy(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let Some((field, spec)) = field_and_spec(v) else { return Ok(Scores::new()) };
    let empty = Map::new();
    let (value, o) = match spec {
        Value::Object(o) => (query_text(o.get("value")), o),
        other => (query_text(Some(other)), &empty),
    };
    check_fuzzy_mapping(mappings, field)?;
    let opts = FuzzyOpts {
        fuzziness: match o.get("fuzziness") {
            Some(f) => parse_fuzziness(f)?,
            None => Fuzziness::Auto(3, 6),
        },
        prefix_length: usize_opt(o, "prefix_length", 0),
        max_expansions: usize_opt(o, "max_expansions", 50),
        transpositions: bool_opt(o, "transpositions", true),
        constant: is_constant_rewrite(o.get("rewrite")),
    };
    // A keyword field's terms are whole values; `_id`/`_index` too.
    let ft = match field {
        "_id" | "_index" => FieldTerms {
            toks: docs
                .iter()
                .map(|d| vec![if field == "_id" { d.id.clone() } else { d.index.clone() }])
                .collect(),
            doc_count: docs.len() as u64,
            avg_len: 1.0,
        },
        _ => FieldTerms::new(mappings, docs, field),
    };
    Ok(ft.fuzzy_scores(&value, &opts, boost_of(o)))
}

/// `match_bool_prefix` on one field: a SHOULD (or MUST, for `and`) term
/// clause per analyzed term -- fuzzy with `fuzziness` -- and a
/// constant-score prefix clause for the last one.
fn bool_prefix_field(
    o: &Map<String, Value>,
    text: &str,
    field: &str,
    mappings: &Value,
    docs: &[CommittedDoc],
    boost: f32,
) -> Result<Scores, EsError> {
    let fuzzy = FuzzyOpts::of_match(o)?;
    let terms = analyzed(o, mappings, field, text);
    let Some((last, head)) = terms.split_last() else { return Ok(Scores::new()) };
    let ft = FieldTerms::new(mappings, docs, field);
    let mut clauses: Vec<Scores> = head
        .iter()
        .map(|t| match &fuzzy {
            Some(f) => ft.fuzzy_scores(t, f, boost),
            None => ft.term_scores(t, ft.doc_freq(t), boost),
        })
        .collect();
    clauses.push(ft.prefix_scores(last, boost));
    combine(clauses, is_and(o), o.get("minimum_should_match"))
}

/// `match_bool_prefix`.
pub fn eval_match_bool_prefix(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let Some((field, spec)) = field_and_spec(v) else { return Ok(Scores::new()) };
    let empty = Map::new();
    let (text, o) = match spec {
        Value::Object(o) => (query_text(o.get("query")), o),
        other => (query_text(Some(other)), &empty),
    };
    bool_prefix_field(o, &text, field, mappings, docs, boost_of(o))
}

/// `field` or `field^boost`.
fn field_boost(spec: &str) -> (&str, f32) {
    match spec.rsplit_once('^') {
        Some((f, b)) => b.parse::<f32>().map_or((spec, 1.0), |b| (f, b)),
        None => (spec, 1.0),
    }
}

/// Whether `multi_match` is handled here: the types and options the
/// basic implementation in `search.rs` doesn't cover.
pub fn handles_multi_match(v: &Value) -> bool {
    let kind = v.get("type").and_then(Value::as_str);
    if matches!(kind, Some("phrase" | "phrase_prefix")) {
        return false;
    }
    matches!(kind, Some("bool_prefix" | "most_fields" | "cross_fields"))
        || ["fuzziness", "minimum_should_match", "tie_breaker", "analyzer", "boost"]
            .iter()
            .any(|k| v.get(*k).is_some())
}

/// `multi_match` of type `best_fields` (dis_max of per-field `match`
/// queries, `tie_breaker` 0), `most_fields` (their sum), `cross_fields`
/// (approximated as `most_fields`) and `bool_prefix` (`match_bool_prefix`
/// per field, best field).
pub fn eval_multi_match(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let Some(o) = v.as_object() else { return Ok(Scores::new()) };
    let kind = o.get("type").and_then(Value::as_str).unwrap_or("best_fields");
    if kind == "bool_prefix" && o.contains_key("slop") {
        return Err(EsError::parsing("[slop] not allowed for type [bool_prefix]"));
    }
    if o.contains_key("fuzziness") && matches!(kind, "cross_fields" | "phrase" | "phrase_prefix") {
        return Err(EsError::parsing(&format!("Fuzziness not allowed for type [{kind}]")));
    }
    let text = query_text(o.get("query"));
    let fields: Vec<(String, f32)> = match o.get("fields") {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .flat_map(|s| {
                let (f, b) = field_boost(s);
                highlight::expand_field_pattern(mappings, f).into_iter().map(move |f| (f, b))
            })
            .collect(),
        Some(Value::String(s)) => {
            let (f, b) = field_boost(s);
            highlight::expand_field_pattern(mappings, f).into_iter().map(|f| (f, b)).collect()
        }
        _ => Vec::new(),
    };
    let tie = o.get("tie_breaker").and_then(Value::as_f64).map(|t| t as f32);
    let boost = boost_of(o);
    let mut per: HashMap<usize, Vec<f32>> = HashMap::new();
    for (field, fb) in &fields {
        let scores = if kind == "bool_prefix" {
            bool_prefix_field(o, &text, field, mappings, docs, fb * boost)?
        } else {
            let mut spec: Map<String, Value> = o
                .iter()
                .filter(|(k, _)| {
                    matches!(
                        k.as_str(),
                        "operator"
                            | "minimum_should_match"
                            | "fuzziness"
                            | "prefix_length"
                            | "max_expansions"
                            | "fuzzy_transpositions"
                            | "fuzzy_rewrite"
                            | "analyzer"
                            | "zero_terms_query"
                    )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            spec.insert("query".into(), Value::String(text.clone()));
            spec.insert("boost".into(), serde_json::json!(fb * boost));
            let q = serde_json::json!({"match": { field.as_str(): spec }});
            match eval(&q, mappings, docs) {
                Ok(s) => s,
                // `lenient`: a field the query can't apply to is skipped.
                Err(_) if o.get("lenient").and_then(Value::as_bool) == Some(true) => continue,
                Err(e) => return Err(e),
            }
        };
        for (i, s) in scores {
            per.entry(i).or_default().push(s);
        }
    }
    let most = matches!(kind, "most_fields" | "cross_fields");
    let tie = tie.unwrap_or(if most { 1.0 } else { 0.0 });
    Ok(per
        .into_iter()
        .map(|(i, ss)| {
            let max = ss.iter().copied().fold(f32::MIN, f32::max);
            let sum: f32 = ss.iter().sum();
            (i, max + tie * (sum - max))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chars(s: &str) -> Vec<char> {
        s.chars().collect()
    }

    #[test]
    fn auto_fuzziness_scales_with_term_length() {
        let auto = parse_fuzziness(&json!("AUTO")).unwrap();
        assert_eq!(auto.edits("ab"), 0);
        assert_eq!(auto.edits("abcd"), 1);
        assert_eq!(auto.edits("abcdef"), 2);
        let custom = parse_fuzziness(&json!("AUTO:2,4")).unwrap();
        assert_eq!(custom.edits("abcd"), 2);
        assert_eq!(parse_fuzziness(&json!("1")).unwrap(), Fuzziness::Edits(1));
        assert!(parse_fuzziness(&json!(3)).is_err());
        assert!(parse_fuzziness(&json!("abc")).is_err());
    }

    #[test]
    fn transpositions_count_as_one_edit_only_when_enabled() {
        assert_eq!(edit_distance(&chars("quikc"), &chars("quick"), true), 1);
        assert_eq!(edit_distance(&chars("quikc"), &chars("quick"), false), 2);
    }

    #[test]
    fn minimum_should_match_forms() {
        assert_eq!(min_should_match(&json!(3), 4).unwrap(), 3);
        assert_eq!(min_should_match(&json!("-1"), 4).unwrap(), 3);
        assert_eq!(min_should_match(&json!("75%"), 4).unwrap(), 3);
        assert_eq!(min_should_match(&json!("-25%"), 4).unwrap(), 3);
        assert_eq!(min_should_match(&json!("3<90%"), 3).unwrap(), 3);
        assert_eq!(min_should_match(&json!("3<90%"), 10).unwrap(), 9);
        assert_eq!(min_should_match(&json!("2<-25% 9<-3"), 12).unwrap(), 9);
    }
}
