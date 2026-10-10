//! Positional queries: `intervals` (`match`, `prefix`, `wildcard`,
//! `fuzzy`, `all_of`, `any_of` with `ordered`/`max_gaps` and the
//! `containing`/`contained_by`/`overlapping`/`before`/`after` filters) and
//! the span queries (`span_term`, `span_near`, `span_or`, `span_not`,
//! `span_first`, `span_containing`, `span_within`, `span_multi`,
//! `field_masking_span`).
//!
//! Both are evaluated the same way: every source yields, per document, the
//! intervals `[start, end]` of term positions where it matches, combined
//! the way Lucene's interval algebra does. Combinations enumerate every
//! choice of sub-intervals (pruned to the widest-covering choice per
//! `[start, end]`), drop those over `max_gaps`, then keep the minimal
//! intervals -- the matches Lucene's minimizing iterators report.
//!
//! Scoring follows Lucene: an `intervals` query scores
//! `boost * f / (f + 1)` with `f` the sum over matches of
//! `1 / max(width - min_extent + 1, 1)`; a span query scores BM25 with the
//! summed idf of its terms and `f = sum(1 / (1 + gaps))` as the frequency.

use std::collections::HashMap;

use serde_json::{Map, Value};

use super::analysis;
use super::fuzzy::{FieldTerms, Fuzziness, FuzzyOpts, parse_fuzziness};
use super::scoring;
use super::search::{
    CommittedDoc, EsError, analyze_for, field_and_spec, query_text, raw_values, resolve_field,
    tokens_for,
};

type Scores = HashMap<usize, f32>;

/// One match: term positions `start..=end` and the positions inside not
/// covered by sub-matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Iv {
    start: i64,
    end: i64,
    gaps: i64,
}

impl Iv {
    fn width(&self) -> i64 {
        self.end - self.start + 1
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Ordered,
    Unordered,
    /// Each sub-match right after the previous one (a phrase).
    Block,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Filter {
    Containing,
    NotContaining,
    ContainedBy,
    NotContainedBy,
    Overlapping,
    NotOverlapping,
    Before,
    After,
}

/// An intervals source.
#[derive(Clone, Debug, PartialEq)]
enum Src {
    /// Any of `terms` in `field` (one term, or a multi-term expansion).
    Terms {
        field: String,
        terms: Vec<String>,
    },
    Combine {
        kind: Kind,
        subs: Vec<Src>,
        max_gaps: Option<i64>,
    },
    Or(Vec<Src>),
    /// `n` successive matches of the same source.
    Repeat(Box<Src>, usize),
    Filtered {
        filter: Filter,
        src: Box<Src>,
        reference: Box<Src>,
    },
    /// `span_first`: matches ending before position `end`.
    First(Box<Src>, i64),
    /// `span_not`: `include` matches with no `exclude` match within
    /// `pre` positions before or `post` after.
    Not {
        include: Box<Src>,
        exclude: Box<Src>,
        pre: i64,
        post: i64,
    },
    /// `span_gap` inside a `span_near`: any `width` positions.
    Gap(i64),
    Nothing,
}

/// Term positions per field of one document (multi-valued fields with
/// Elasticsearch's 100-position gap between values).
struct Positions<'a> {
    mappings: &'a Value,
    source: &'a Value,
    by_field: HashMap<String, (HashMap<String, Vec<i64>>, i64)>,
}

impl Positions<'_> {
    fn field(&mut self, field: &str) -> &(HashMap<String, Vec<i64>>, i64) {
        if !self.by_field.contains_key(field) {
            let (path, ty) = resolve_field(self.mappings, field);
            let mut map: HashMap<String, Vec<i64>> = HashMap::new();
            let mut pos: i64 = 0;
            // Text fields: the analysis engine's positions (stopword and
            // synonym gaps, `position_increment_gap` between values).
            if matches!(ty.as_deref(), None | Some("text" | "match_only_text")) {
                let texts: Vec<String> = raw_values(self.source, &path)
                    .into_iter()
                    .filter_map(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        Value::Number(n) => Some(n.to_string()),
                        Value::Bool(b) => Some(b.to_string()),
                        _ => None,
                    })
                    .collect();
                let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
                for (t, p) in
                    analysis::field_positions(self.mappings, field, &refs, analysis::Mode::Index)
                {
                    pos = pos.max(p + 1);
                    map.entry(t).or_default().push(p);
                }
                self.by_field.insert(field.to_string(), (map, pos));
                return &self.by_field[field];
            }
            for (k, v) in raw_values(self.source, &path).into_iter().enumerate() {
                let mut wrapped = v.clone();
                for seg in path.split('.').rev() {
                    let mut m = Map::new();
                    m.insert(seg.to_string(), wrapped);
                    wrapped = Value::Object(m);
                }
                let toks = tokens_for(self.mappings, &wrapped, field);
                if k > 0 && !toks.is_empty() && pos > 0 {
                    pos += 100;
                }
                for t in toks {
                    map.entry(t).or_default().push(pos);
                    pos += 1;
                }
            }
            self.by_field.insert(field.to_string(), (map, pos));
        }
        &self.by_field[field]
    }
}

/// Keeps the minimal intervals: none containing another (the one with
/// fewer gaps among equal ones).
fn minimize(mut ivs: Vec<Iv>) -> Vec<Iv> {
    ivs.sort();
    ivs.dedup_by(|b, a| a.start == b.start && a.end == b.end);
    let all = ivs.clone();
    ivs.retain(|a| {
        !all.iter()
            .any(|b| (b.start, b.end) != (a.start, a.end) && b.start >= a.start && b.end <= a.end)
    });
    ivs
}

/// Every combination of one interval per list (in order for `Ordered` and
/// `Block`), as `(start, end) -> widest summed sub-width`.
fn combine(kind: Kind, lists: &[Vec<Iv>]) -> HashMap<(i64, i64), i64> {
    let mut states: HashMap<(i64, i64), i64> = HashMap::new();
    for iv in &lists[0] {
        let e = states.entry((iv.start, iv.end)).or_insert(i64::MIN);
        *e = (*e).max(iv.width());
    }
    for list in &lists[1..] {
        let mut next: HashMap<(i64, i64), i64> = HashMap::new();
        for (&(s, e), &w) in &states {
            for iv in list {
                let key = match kind {
                    Kind::Ordered if iv.start > e => (s, iv.end),
                    Kind::Block if iv.start == e + 1 => (s, iv.end),
                    Kind::Unordered => (s.min(iv.start), e.max(iv.end)),
                    _ => continue,
                };
                let slot = next.entry(key).or_insert(i64::MIN);
                *slot = (*slot).max(w + iv.width());
            }
        }
        // Never let a pathological document blow up.
        if next.len() > 200_000 {
            let mut v: Vec<_> = next.into_iter().collect();
            v.sort_by_key(|((s, e), _)| (e - s, *s));
            v.truncate(200_000);
            next = v.into_iter().collect();
        }
        states = next;
        if states.is_empty() {
            break;
        }
    }
    states
}

impl Src {
    /// Lucene's `minExtent`: the fewest positions a match can span.
    fn min_extent(&self) -> i64 {
        match self {
            Src::Terms { .. } => 1,
            Src::Combine { subs, .. } => subs.iter().map(Src::min_extent).sum(),
            Src::Or(subs) => subs.iter().map(Src::min_extent).min().unwrap_or(0),
            Src::Repeat(s, _) => s.min_extent(),
            Src::Filtered { src, .. } | Src::First(src, _) => src.min_extent(),
            Src::Not { include, .. } => include.min_extent(),
            Src::Gap(w) => *w,
            Src::Nothing => 0,
        }
    }

    /// Every leaf `(field, term)`, for span scoring statistics.
    fn leaf_terms(&self, out: &mut Vec<(String, String)>) {
        match self {
            Src::Terms { field, terms } => {
                out.extend(terms.iter().map(|t| (field.clone(), t.clone())))
            }
            Src::Combine { subs, .. } | Src::Or(subs) => {
                subs.iter().for_each(|s| s.leaf_terms(out))
            }
            Src::Repeat(s, _) | Src::First(s, _) => s.leaf_terms(out),
            // `span_containing`/`span_within` weigh both sides' terms.
            Src::Filtered { src, reference, .. } => {
                src.leaf_terms(out);
                reference.leaf_terms(out);
            }
            Src::Not { include, .. } => include.leaf_terms(out),
            Src::Gap(_) | Src::Nothing => {}
        }
    }

    /// Lucene's span `width()` of a match: 0 for a term, the gaps of an
    /// ordered near, the whole length of an unordered one.
    fn span_width(&self, iv: &Iv) -> i64 {
        match self {
            Src::Terms { .. } | Src::Gap(_) | Src::Nothing => 0,
            Src::Combine { kind: Kind::Unordered, .. } => iv.width(),
            Src::Combine { .. } | Src::Repeat(..) => iv.gaps,
            Src::Or(subs) => {
                if subs.iter().all(|s| matches!(s, Src::Terms { .. })) {
                    0
                } else {
                    iv.gaps
                }
            }
            Src::Filtered { src, .. } | Src::First(src, _) => src.span_width(iv),
            Src::Not { include, .. } => include.span_width(iv),
        }
    }

    fn intervals(&self, pos: &mut Positions) -> Vec<Iv> {
        match self {
            Src::Terms { field, terms } => {
                let (map, _) = pos.field(field);
                let mut out: Vec<Iv> = terms
                    .iter()
                    .filter_map(|t| map.get(t))
                    .flatten()
                    .map(|&p| Iv { start: p, end: p, gaps: 0 })
                    .collect();
                out.sort();
                out.dedup();
                out
            }
            Src::Gap(w) => {
                let len = self_len(pos);
                (0..len.max(0)).map(|p| Iv { start: p, end: p + w - 1, gaps: 0 }).collect()
            }
            Src::Nothing => vec![],
            Src::Or(subs) => {
                let mut out: Vec<Iv> = subs.iter().flat_map(|s| s.intervals(pos)).collect();
                out.sort();
                out.dedup_by(|b, a| a.start == b.start && a.end == b.end);
                out
            }
            Src::Repeat(src, n) => {
                let ivs = src.intervals(pos);
                let mut out = Vec::new();
                for w in ivs.windows(*n) {
                    if w.windows(2).any(|p| p[1].start <= p[0].end) {
                        continue;
                    }
                    let start = w[0].start;
                    let end = w.iter().map(|i| i.end).max().unwrap_or(start);
                    let sum: i64 = w.iter().map(Iv::width).sum();
                    out.push(Iv { start, end, gaps: end - start + 1 - sum });
                }
                out
            }
            Src::Combine { kind, subs, max_gaps } => {
                let lists: Vec<Vec<Iv>> = subs.iter().map(|s| s.intervals(pos)).collect();
                if lists.is_empty() || lists.iter().any(Vec::is_empty) {
                    return vec![];
                }
                let ivs: Vec<Iv> = combine(*kind, &lists)
                    .into_iter()
                    .map(|((s, e), w)| Iv { start: s, end: e, gaps: e - s + 1 - w })
                    .filter(|iv| max_gaps.is_none_or(|m| iv.gaps <= m))
                    .collect();
                minimize(ivs)
            }
            Src::Filtered { filter, src, reference } => {
                let ivs = src.intervals(pos);
                let refs = reference.intervals(pos);
                ivs.into_iter()
                    .filter(|a| {
                        let any = |f: &dyn Fn(&Iv) -> bool| refs.iter().any(f);
                        match filter {
                            Filter::Containing => any(&|b| a.start <= b.start && b.end <= a.end),
                            Filter::NotContaining => {
                                !any(&|b| a.start <= b.start && b.end <= a.end)
                            }
                            Filter::ContainedBy => any(&|b| b.start <= a.start && a.end <= b.end),
                            Filter::NotContainedBy => {
                                !any(&|b| b.start <= a.start && a.end <= b.end)
                            }
                            Filter::Overlapping => any(&|b| a.start <= b.end && b.start <= a.end),
                            Filter::NotOverlapping => {
                                !any(&|b| a.start <= b.end && b.start <= a.end)
                            }
                            Filter::Before => any(&|b| a.end < b.start),
                            Filter::After => any(&|b| a.start > b.end),
                        }
                    })
                    .collect()
            }
            Src::First(src, end) => {
                src.intervals(pos).into_iter().filter(|a| a.end < *end).collect()
            }
            Src::Not { include, exclude, pre, post } => {
                let ex = exclude.intervals(pos);
                include
                    .intervals(pos)
                    .into_iter()
                    .filter(|a| {
                        !ex.iter().any(|b| b.start <= a.end + post && b.end >= a.start - pre)
                    })
                    .collect()
            }
        }
    }
}

/// The highest position of any field read so far (for `span_gap`).
fn self_len(pos: &Positions) -> i64 {
    pos.by_field.values().map(|(_, n)| *n).max().unwrap_or(0)
}

/// Builds a combination the way Lucene does: nested combinations of the
/// same kind without their own `max_gaps` are flattened, and repeated
/// sub-sources (consecutive ones when ordered) become a `Repeat`.
fn build_combine(kind: Kind, subs: Vec<Src>, max_gaps: Option<i64>) -> Src {
    let mut flat = Vec::new();
    for s in subs {
        match s {
            Src::Combine { kind: k, subs: inner, max_gaps: None } if k == kind => {
                flat.extend(inner)
            }
            other => flat.push(other),
        }
    }
    let mut grouped: Vec<(Src, usize)> = Vec::new();
    for s in flat {
        let same = match kind {
            Kind::Unordered => grouped.iter_mut().find(|(g, _)| *g == s),
            _ => grouped.last_mut().filter(|(g, _)| *g == s),
        };
        match same {
            Some((_, n)) => *n += 1,
            None => grouped.push((s, 1)),
        }
    }
    let subs: Vec<Src> = grouped
        .into_iter()
        .map(|(s, n)| if n > 1 { Src::Repeat(Box::new(s), n) } else { s })
        .collect();
    if subs.len() == 1 {
        let only = subs.into_iter().next().unwrap();
        return match max_gaps {
            None => only,
            Some(_) => Src::Combine { kind, subs: vec![only], max_gaps },
        };
    }
    Src::Combine { kind, subs, max_gaps }
}

/// ES's `IntervalBuilder.combineSources`: `max_gaps: 0` and `ordered` is a
/// phrase.
fn combine_sources(subs: Vec<Src>, max_gaps: i64, ordered: bool) -> Src {
    match subs.len() {
        0 => Src::Nothing,
        1 => subs.into_iter().next().unwrap(),
        _ => {
            if max_gaps == 0 && ordered {
                return build_combine(Kind::Block, subs, None);
            }
            let kind = if ordered { Kind::Ordered } else { Kind::Unordered };
            build_combine(kind, subs, (max_gaps >= 0).then_some(max_gaps))
        }
    }
}

// --- `intervals` --------------------------------------------------------

struct Ctx<'a> {
    mappings: &'a Value,
    docs: &'a [CommittedDoc],
    terms: HashMap<String, FieldTerms>,
}

impl Ctx<'_> {
    fn field_terms(&mut self, field: &str) -> &FieldTerms {
        if !self.terms.contains_key(field) {
            let ft = FieldTerms::new(self.mappings, self.docs, field);
            self.terms.insert(field.to_string(), ft);
        }
        &self.terms[field]
    }
}

const RULES: &str = "[match, any_of, all_of, prefix, wildcard]";

fn rule_of(v: &Value) -> Result<(&str, &Value), EsError> {
    let Some(o) = v.as_object() else {
        return Err(EsError::parsing("Expected [START_OBJECT] for an interval rule"));
    };
    let mut rules = o.iter().filter(|(k, _)| k.as_str() != "boost" && k.as_str() != "_name");
    let Some((name, body)) = rules.next() else {
        return Err(EsError::parsing("Expected [FIELD_NAME] but got [END_OBJECT]"));
    };
    if let Some((second, _)) = rules.next() {
        return Err(EsError::parsing(&format!(
            "Only one interval rule can be specified, found [{name}] and [{second}]"
        )));
    }
    Ok((name.as_str(), body))
}

fn int_opt(o: &Map<String, Value>, key: &str, default: i64) -> i64 {
    match o.get(key) {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(default),
        Some(Value::String(s)) => s.parse().unwrap_or(default),
        _ => default,
    }
}

fn bool_opt(o: &Map<String, Value>, key: &str) -> bool {
    match o.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

/// The text normalized the way a field's analyzer normalizes a
/// multi-term query (lowercased for text fields).
fn normalize(o: &Map<String, Value>, mappings: &Value, field: &str, text: &str) -> String {
    match o.get("analyzer").and_then(Value::as_str) {
        Some("whitespace" | "keyword") => text.to_string(),
        Some(_) => text.to_lowercase(),
        None => match resolve_field(mappings, field).1.as_deref() {
            None | Some("text" | "match_only_text") => text.to_lowercase(),
            _ => text.to_string(),
        },
    }
}

fn glob_regex(pattern: &str) -> Option<regex_lite::Regex> {
    let mut re = String::from("^");
    for c in pattern.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            c => re.push_str(&regex_lite::escape(&c.to_string())),
        }
    }
    re.push('$');
    regex_lite::Regex::new(&re).ok()
}

const MAX_EXPANSIONS: usize = 128;

fn too_many(pattern: &str) -> EsError {
    EsError::shard_failure(
        "illegal_state_exception",
        &format!("Automaton [{pattern}] expanded to too many terms (limit {MAX_EXPANSIONS})"),
    )
}

/// The parameters each interval rule takes.
const RULE_PARAMS: &[(&str, &[&str])] = &[
    ("match", &["query", "max_gaps", "ordered", "analyzer", "filter", "use_field"]),
    ("any_of", &["intervals", "filter"]),
    ("all_of", &["intervals", "max_gaps", "ordered", "filter"]),
    ("prefix", &["prefix", "analyzer", "use_field"]),
    ("wildcard", &["pattern", "analyzer", "use_field"]),
    ("fuzzy", &["term", "prefix_length", "transpositions", "fuzziness", "analyzer", "use_field"]),
];

fn parse_rule(name: &str, body: &Value, field: &str, cx: &mut Ctx) -> Result<Src, EsError> {
    let empty = Map::new();
    let o = body.as_object().unwrap_or(&empty);
    if let Some((_, params)) = RULE_PARAMS.iter().find(|(r, _)| *r == name)
        && let Some(bad) = o.keys().find(|k| !params.contains(&k.as_str()))
    {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            &format!("[{name}] unknown field [{bad}]"),
        ));
    }
    let use_field = o.get("use_field").and_then(Value::as_str).unwrap_or(field).to_string();
    if use_field != field {
        check_text_field(cx.mappings, &use_field)?;
    }
    let src = match name {
        "match" => {
            let text = query_text(o.get("query"));
            let toks = match o.get("analyzer").and_then(Value::as_str) {
                Some(a) => analysis::analyzer(a).terms(&text),
                None => analyze_for(cx.mappings, &use_field, &text),
            };
            let subs: Vec<Src> = toks
                .into_iter()
                .map(|t| Src::Terms { field: use_field.clone(), terms: vec![t] })
                .collect();
            combine_sources(subs, int_opt(o, "max_gaps", -1), bool_opt(o, "ordered"))
        }
        "any_of" | "all_of" => {
            let list = o.get("intervals").and_then(Value::as_array).cloned().unwrap_or_default();
            if list.is_empty() {
                return Err(EsError::parsing(&format!(
                    "[{name}] requires at least one interval rule"
                )));
            }
            let subs = list
                .iter()
                .map(|r| {
                    let (n, b) = rule_of(r)?;
                    parse_rule(n, b, field, cx)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if name == "any_of" {
                if subs.len() == 1 { subs.into_iter().next().unwrap() } else { Src::Or(subs) }
            } else {
                combine_sources(subs, int_opt(o, "max_gaps", -1), bool_opt(o, "ordered"))
            }
        }
        "prefix" => {
            let prefix = normalize(o, cx.mappings, &use_field, &query_text(o.get("prefix")));
            let terms: Vec<String> = cx
                .field_terms(&use_field)
                .terms()
                .into_iter()
                .filter(|t| t.starts_with(&prefix))
                .cloned()
                .collect();
            if terms.len() > MAX_EXPANSIONS {
                return Err(too_many(&format!("{prefix}*")));
            }
            Src::Terms { field: use_field.clone(), terms }
        }
        "wildcard" => {
            let pattern = normalize(o, cx.mappings, &use_field, &query_text(o.get("pattern")));
            let re = glob_regex(&pattern);
            let terms: Vec<String> = cx
                .field_terms(&use_field)
                .terms()
                .into_iter()
                .filter(|t| re.as_ref().is_some_and(|r| r.is_match(t)))
                .cloned()
                .collect();
            if terms.len() > MAX_EXPANSIONS {
                return Err(too_many(&pattern));
            }
            Src::Terms { field: use_field.clone(), terms }
        }
        "fuzzy" => {
            let term = normalize(o, cx.mappings, &use_field, &query_text(o.get("term")));
            let opts = FuzzyOpts {
                fuzziness: match o.get("fuzziness") {
                    Some(f) => parse_fuzziness(f)?,
                    None => Fuzziness::Auto(3, 6),
                },
                prefix_length: int_opt(o, "prefix_length", 0).max(0) as usize,
                max_expansions: MAX_EXPANSIONS,
                transpositions: o.get("transpositions").and_then(Value::as_bool).unwrap_or(true),
                constant: false,
            };
            let terms = cx
                .field_terms(&use_field)
                .fuzzy_expansions(&term, &opts)
                .into_iter()
                .map(|(t, _)| t)
                .collect();
            Src::Terms { field: use_field.clone(), terms }
        }
        other => {
            return Err(EsError::parsing(&format!(
                "Unknown interval type [{other}], expecting one of {RULES}"
            )));
        }
    };
    match o.get("filter") {
        Some(f) => apply_filter(src, f, field, cx),
        None => Ok(src),
    }
}

fn apply_filter(src: Src, spec: &Value, field: &str, cx: &mut Ctx) -> Result<Src, EsError> {
    let Some(fo) = spec.as_object() else { return Ok(src) };
    let Some((kind, rule)) = fo.iter().next() else { return Ok(src) };
    let filter = match kind.as_str() {
        "containing" => Filter::Containing,
        "not_containing" => Filter::NotContaining,
        "contained_by" => Filter::ContainedBy,
        "not_contained_by" => Filter::NotContainedBy,
        "overlapping" => Filter::Overlapping,
        "not_overlapping" => Filter::NotOverlapping,
        "before" => Filter::Before,
        "after" => Filter::After,
        "script" => {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                "script interval filters are not supported",
            ));
        }
        other => {
            return Err(EsError::parsing(&format!("Unknown filter type [{other}]")));
        }
    };
    let (n, b) = rule_of(rule)?;
    let reference = parse_rule(n, b, field, cx)?;
    Ok(Src::Filtered { filter, src: Box::new(src), reference: Box::new(reference) })
}

/// Interval queries run on text fields only.
fn check_text_field(mappings: &Value, field: &str) -> Result<(), EsError> {
    let ty = resolve_field(mappings, field).1;
    if let Some(t) = ty.as_deref().filter(|t| !matches!(*t, "text" | "match_only_text")) {
        let reason = format!(
            "Can only use interval queries on text fields - not on [{field}] which is of type \
             [{t}]"
        );
        return Err(EsError::shard_failure(
            "query_shard_exception",
            &format!("failed to create query: {reason}"),
        )
        .caused_by("illegal_argument_exception", &reason));
    }
    Ok(())
}

/// `intervals`.
pub fn eval_intervals(
    v: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let Some((field, spec)) = field_and_spec(v) else {
        return Err(EsError::parsing("Expected [FIELD_NAME] but got [END_OBJECT]"));
    };
    check_text_field(mappings, field)?;
    let (name, body) = rule_of(spec)?;
    let boost = spec.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32;
    let mut cx = Ctx { mappings, docs, terms: HashMap::new() };
    let src = parse_rule(name, body, field, &mut cx)?;
    let min_extent = src.min_extent();
    let mut out = Scores::new();
    for (i, d) in docs.iter().enumerate() {
        let mut pos = Positions { mappings, source: &d.source, by_field: HashMap::new() };
        let ivs = src.intervals(&mut pos);
        if ivs.is_empty() {
            continue;
        }
        let f: f32 = ivs.iter().map(|iv| 1.0 / (iv.width() - min_extent + 1).max(1) as f32).sum();
        out.insert(i, boost * f / (f + 1.0));
    }
    Ok(out)
}

// --- span queries -------------------------------------------------------

pub const SPAN_QUERIES: &[&str] = &[
    "span_term",
    "span_near",
    "span_or",
    "span_not",
    "span_first",
    "span_containing",
    "span_within",
    "span_multi",
    "field_masking_span",
];

fn span_error(reason: &str) -> EsError {
    EsError::shard_failure("query_shard_exception", &format!("failed to create query: {reason}"))
        .caused_by("illegal_argument_exception", reason)
}

/// The single `{type: body}` of a span clause.
fn span_clause(v: &Value) -> Result<(&str, &Value), EsError> {
    match v.as_object().and_then(|o| o.iter().next()) {
        Some((k, b)) if SPAN_QUERIES.contains(&k.as_str()) || k == "span_gap" => Ok((k, b)),
        Some((k, _)) => Err(EsError::parsing(&format!(
            "spanNear [clauses] must be of type span query, got [{k}]"
        ))),
        None => Err(EsError::parsing("span clause must be an object")),
    }
}

fn span_term_value(spec: &Value) -> String {
    match spec {
        Value::Object(o) => query_text(o.get("value").or_else(|| o.get("term"))),
        other => query_text(Some(other)),
    }
}

/// A span query as a source and the field it reports (`field_masking_span`
/// changes the latter).
fn parse_span(kind: &str, body: &Value, cx: &mut Ctx) -> Result<(Src, String), EsError> {
    match kind {
        "span_term" => {
            let Some((field, spec)) = field_and_spec(body) else {
                return Err(EsError::parsing("[span_term] query malformed, no field"));
            };
            Ok((
                Src::Terms { field: field.to_string(), terms: vec![span_term_value(spec)] },
                field.to_string(),
            ))
        }
        "span_multi" => {
            let inner = body.get("match").ok_or_else(|| {
                EsError::parsing("[span_multi] must have [match] multi term query clause")
            })?;
            let (qtype, qbody) =
                inner.as_object().and_then(|o| o.iter().next()).ok_or_else(|| {
                    EsError::parsing("[span_multi] must have [match] multi term query clause")
                })?;
            let Some((field, spec)) = field_and_spec(qbody) else {
                return Err(EsError::parsing("[span_multi] malformed inner query"));
            };
            let ft = cx.field_terms(field);
            let all = ft.terms();
            let terms: Vec<String> = match qtype.as_str() {
                "prefix" => {
                    let p = span_term_value(spec);
                    all.into_iter().filter(|t| t.starts_with(&p)).cloned().collect()
                }
                "wildcard" => {
                    let p = match spec {
                        Value::Object(o) => {
                            query_text(o.get("value").or_else(|| o.get("wildcard")))
                        }
                        other => query_text(Some(other)),
                    };
                    let re = glob_regex(&p);
                    all.into_iter()
                        .filter(|t| re.as_ref().is_some_and(|r| r.is_match(t)))
                        .cloned()
                        .collect()
                }
                "regexp" => {
                    let p = span_term_value(spec);
                    let re = regex_lite::Regex::new(&format!("^(?:{p})$")).ok();
                    all.into_iter()
                        .filter(|t| re.as_ref().is_some_and(|r| r.is_match(t)))
                        .cloned()
                        .collect()
                }
                "fuzzy" => {
                    let empty = Map::new();
                    let o = spec.as_object().unwrap_or(&empty);
                    let opts = FuzzyOpts {
                        fuzziness: match o.get("fuzziness") {
                            Some(f) => parse_fuzziness(f)?,
                            None => Fuzziness::Auto(3, 6),
                        },
                        prefix_length: int_opt(o, "prefix_length", 0).max(0) as usize,
                        max_expansions: int_opt(o, "max_expansions", 50).max(1) as usize,
                        transpositions: o
                            .get("transpositions")
                            .and_then(Value::as_bool)
                            .unwrap_or(true),
                        constant: false,
                    };
                    ft.fuzzy_expansions(&span_term_value(spec), &opts)
                        .into_iter()
                        .map(|(t, _)| t)
                        .collect()
                }
                "range" => {
                    let s = |k: &str| spec.get(k).map(|v| query_text(Some(v)));
                    let (gte, gt, lte, lt) = (s("gte"), s("gt"), s("lte"), s("lt"));
                    all.into_iter()
                        .filter(|t| {
                            gte.as_ref().is_none_or(|b| t.as_str() >= b.as_str())
                                && gt.as_ref().is_none_or(|b| t.as_str() > b.as_str())
                                && lte.as_ref().is_none_or(|b| t.as_str() <= b.as_str())
                                && lt.as_ref().is_none_or(|b| t.as_str() < b.as_str())
                        })
                        .cloned()
                        .collect()
                }
                "term" => vec![span_term_value(spec)],
                other => {
                    return Err(EsError::parsing(&format!(
                        "[span_multi] [match] must be of type multi term query, got [{other}]"
                    )));
                }
            };
            Ok((Src::Terms { field: field.to_string(), terms }, field.to_string()))
        }
        "span_near" => {
            let clauses =
                body.get("clauses").and_then(Value::as_array).cloned().unwrap_or_default();
            if clauses.is_empty() {
                return Err(EsError::parsing("span_near must include [clauses]"));
            }
            let slop = body.get("slop").and_then(Value::as_i64).unwrap_or(0);
            let in_order = body.get("in_order").and_then(Value::as_bool).unwrap_or(true);
            let mut subs = Vec::new();
            let mut field: Option<String> = None;
            for c in &clauses {
                let (k, b) = span_clause(c)?;
                if k == "span_gap" {
                    let w = b
                        .as_object()
                        .and_then(|o| o.values().next())
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    subs.push(Src::Gap(w));
                    continue;
                }
                let (s, f) = parse_span(k, b, cx)?;
                match &field {
                    Some(first) if *first != f => {
                        let mut leaves = Vec::new();
                        s.leaf_terms(&mut leaves);
                        let shown =
                            leaves.first().map(|(lf, t)| format!("{lf}:{t}")).unwrap_or_default();
                        return Err(span_error(&format!(
                            "Cannot add clause {shown} to SpanNearQuery for field {first}"
                        )));
                    }
                    None => field = Some(f),
                    _ => {}
                }
                subs.push(s);
            }
            let kind = if in_order { Kind::Ordered } else { Kind::Unordered };
            let src = if subs.len() == 1 {
                subs.pop().unwrap()
            } else {
                Src::Combine { kind, subs, max_gaps: Some(slop) }
            };
            Ok((src, field.unwrap_or_default()))
        }
        "span_or" => {
            let clauses =
                body.get("clauses").and_then(Value::as_array).cloned().unwrap_or_default();
            if clauses.is_empty() {
                return Err(EsError::parsing("spanOr must include [clauses]"));
            }
            let mut subs = Vec::new();
            let mut field = String::new();
            for c in &clauses {
                let (k, b) = span_clause(c)?;
                let (s, f) = parse_span(k, b, cx)?;
                field = f;
                subs.push(s);
            }
            Ok((Src::Or(subs), field))
        }
        "span_not" => {
            let inc = body.get("include").ok_or_else(|| {
                EsError::parsing("span_not must have [include] span query clause")
            })?;
            let exc = body.get("exclude").ok_or_else(|| {
                EsError::parsing("span_not must have [exclude] span query clause")
            })?;
            let (ik, ib) = span_clause(inc)?;
            let (ek, eb) = span_clause(exc)?;
            let (include, field) = parse_span(ik, ib, cx)?;
            let (exclude, _) = parse_span(ek, eb, cx)?;
            let dist = body.get("dist").and_then(Value::as_i64);
            let pre = dist.or_else(|| body.get("pre").and_then(Value::as_i64)).unwrap_or(0);
            let post = dist.or_else(|| body.get("post").and_then(Value::as_i64)).unwrap_or(0);
            Ok((
                Src::Not { include: Box::new(include), exclude: Box::new(exclude), pre, post },
                field,
            ))
        }
        "span_first" => {
            let m = body.get("match").ok_or_else(|| {
                EsError::parsing("span_first must have [match] span query clause")
            })?;
            let end = body
                .get("end")
                .and_then(Value::as_i64)
                .ok_or_else(|| EsError::parsing("span_first must have [end] set for it"))?;
            let (k, b) = span_clause(m)?;
            let (src, field) = parse_span(k, b, cx)?;
            Ok((Src::First(Box::new(src), end), field))
        }
        "span_containing" | "span_within" => {
            let big = body
                .get("big")
                .ok_or_else(|| EsError::parsing(&format!("{kind} must include [big]")))?;
            let little = body
                .get("little")
                .ok_or_else(|| EsError::parsing(&format!("{kind} must include [little]")))?;
            let (bk, bb) = span_clause(big)?;
            let (lk, lb) = span_clause(little)?;
            let (big, bf) = parse_span(bk, bb, cx)?;
            let (little, lf) = parse_span(lk, lb, cx)?;
            Ok(if kind == "span_containing" {
                (
                    Src::Filtered {
                        filter: Filter::Containing,
                        src: Box::new(big),
                        reference: Box::new(little),
                    },
                    bf,
                )
            } else {
                (
                    Src::Filtered {
                        filter: Filter::ContainedBy,
                        src: Box::new(little),
                        reference: Box::new(big),
                    },
                    lf,
                )
            })
        }
        "field_masking_span" => {
            let q = body.get("query").ok_or_else(|| {
                EsError::parsing("field_masking_span must have [query] span query clause")
            })?;
            let field = body.get("field").and_then(Value::as_str).ok_or_else(|| {
                EsError::parsing("field_masking_span must have [field] set for it")
            })?;
            let (k, b) = span_clause(q)?;
            let (src, _) = parse_span(k, b, cx)?;
            Ok((src, field.to_string()))
        }
        other => Err(EsError::parsing(&format!("unknown query [{other}]"))),
    }
}

/// A span query: BM25 over the summed idf of its terms, with the sloppy
/// frequency of its matches.
pub fn eval_span(
    kind: &str,
    body: &Value,
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<Scores, EsError> {
    let mut cx = Ctx { mappings, docs, terms: HashMap::new() };
    let (src, field) = parse_span(kind, body, &mut cx)?;
    let boost = span_boost(kind, body);
    let mut leaves = Vec::new();
    src.leaf_terms(&mut leaves);
    leaves.sort();
    leaves.dedup();
    let ft = cx.field_terms(&field);
    let doc_count = ft.toks.iter().filter(|t| !t.is_empty()).count() as u64;
    let total: u64 = ft.toks.iter().map(|t| t.len() as u64).sum();
    let avg = if doc_count > 0 { total as f32 / doc_count as f32 } else { 1.0 };
    let lens: Vec<u32> = ft.toks.iter().map(|t| t.len() as u32).collect();
    let mut idf = 0.0f32;
    for (f, t) in &leaves {
        let ftf = cx.field_terms(f);
        let n = ftf.toks.iter().filter(|x| !x.is_empty()).count() as u64;
        let df = ftf.doc_freq(t);
        if df > 0 {
            idf += scoring::idf(df, n.max(1));
        }
    }
    let mut out = Scores::new();
    for (i, d) in docs.iter().enumerate() {
        let mut pos = Positions { mappings, source: &d.source, by_field: HashMap::new() };
        let ivs = src.intervals(&mut pos);
        if ivs.is_empty() {
            continue;
        }
        let freq: f32 = ivs.iter().map(|iv| 1.0 / (1 + src.span_width(iv).max(0)) as f32).sum();
        let len = scoring::norm_doc_len(lens.get(i).copied().unwrap_or(0)).max(1) as f32;
        let norm = scoring::K1 * ((1.0 - scoring::B) + scoring::B * len / avg);
        out.insert(i, boost * idf * freq * (scoring::K1 + 1.0) / (freq + norm));
    }
    Ok(out)
}

fn span_boost(kind: &str, body: &Value) -> f32 {
    let b = if kind == "span_term" {
        field_and_spec(body).and_then(|(_, s)| s.get("boost"))
    } else {
        body.get("boost")
    };
    b.and_then(Value::as_f64).unwrap_or(1.0) as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn docs(texts: &[&str]) -> Vec<CommittedDoc> {
        texts
            .iter()
            .enumerate()
            .map(|(i, t)| CommittedDoc {
                index: "i".into(),
                id: (i + 1).to_string(),
                source: json!({"text": t}),
                version: 1,
                seq: i as i64,
                full_source: None,
                tsid: None,
            })
            .collect()
    }

    fn hits(q: Value, d: &[CommittedDoc]) -> Vec<usize> {
        let m = json!({"properties": {"text": {"type": "text"}}});
        let mut v: Vec<usize> = eval_intervals(&q, &m, d).unwrap().into_keys().collect();
        v.sort();
        v
    }

    #[test]
    fn ordered_unordered_and_phrase_matching() {
        let d = docs(&["its cold outside", "baby its cold there outside", "outside it is cold"]);
        assert_eq!(
            hits(json!({"text": {"match": {"query": "cold outside", "ordered": true}}}), &d),
            vec![0, 1]
        );
        assert_eq!(hits(json!({"text": {"match": {"query": "cold outside"}}}), &d), vec![0, 1, 2]);
        assert_eq!(
            hits(
                json!({"text": {"match": {"query": "cold outside", "ordered": true, "max_gaps": 0}}}),
                &d
            ),
            vec![0]
        );
    }

    #[test]
    fn disjunctions_inside_a_phrase_try_every_alternative() {
        let d = docs(&["the big bad wolf", "the big wolf"]);
        let q = json!({"text": {"all_of": {"ordered": true, "max_gaps": 0, "intervals": [
            {"match": {"query": "the"}},
            {"any_of": {"intervals": [{"match": {"query": "big"}}, {"match": {"query": "big bad"}}]}},
            {"match": {"query": "wolf"}}]}}});
        let m = json!({"properties": {"text": {"type": "text"}}});
        let s = eval_intervals(&q, &m, &d).unwrap();
        assert!(s[&1] > s[&0], "the tighter match scores higher: {s:?}");
    }

    #[test]
    fn filters_relate_two_sources() {
        let d = docs(&["its cold outside", "outside it is cold"]);
        let q = json!({"text": {"match": {"query": "cold", "filter": {"before": {"match": {"query": "outside"}}}}}});
        assert_eq!(hits(q, &d), vec![0]);
        let q = json!({"text": {"match": {"query": "cold", "filter": {"after": {"match": {"query": "outside"}}}}}});
        assert_eq!(hits(q, &d), vec![1]);
    }
}
