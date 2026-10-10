//! `more_like_this`: Lucene's `MoreLikeThis` term selection over the
//! `like` texts and documents (minus the `unlike` ones' terms), searched
//! as a disjunction of term queries with `minimum_should_match` (30%).
//!
//! Like/unlike documents named by `_id` are fetched before the search
//! (`dsl::prepare`, which embeds their source as `doc`); one in the
//! searched index is also found here when it wasn't.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::analysis;
use super::fuzzy::{FieldTerms, min_should_match};
use super::search::{CommittedDoc, EsError, analyze_for, resolve_field, tokens_for};

type Scores = HashMap<usize, f32>;

/// One `like`/`unlike` entry.
enum Item {
    Text(String),
    Doc {
        source: Value,
        index: Option<String>,
        id: Option<String>,
        fields: Option<Vec<String>>,
    },
    /// A document that doesn't exist: contributes nothing.
    Missing,
}

fn items(v: Option<&Value>, docs: &[CommittedDoc]) -> Vec<Item> {
    let list: Vec<&Value> = match v {
        None | Some(Value::Null) => vec![],
        Some(Value::Array(a)) => a.iter().collect(),
        Some(other) => vec![other],
    };
    list.into_iter()
        .map(|x| match x {
            Value::String(s) => Item::Text(s.clone()),
            Value::Object(o) => {
                let index = o.get("_index").and_then(Value::as_str).map(str::to_string);
                let id = o.get("_id").and_then(Value::as_str).map(str::to_string);
                let fields = o
                    .get("fields")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect());
                let source = match (o.get("doc"), &id) {
                    (Some(d), _) => Some(d.clone()),
                    (None, Some(id)) => docs
                        .iter()
                        .find(|d| d.id == *id && index.as_ref().is_none_or(|i| *i == d.index))
                        .map(|d| d.full().clone()),
                    (None, None) => None,
                };
                match source {
                    Some(source) => Item::Doc { source, index, id, fields },
                    None => Item::Missing,
                }
            }
            _ => Item::Missing,
        })
        .collect()
}

/// The text and keyword fields an unqualified `more_like_this` reads.
fn default_fields(mappings: &Value) -> Vec<String> {
    fn walk(props: Option<&Value>, prefix: &str, out: &mut Vec<String>) {
        let Some(obj) = props.and_then(Value::as_object) else { return };
        for (name, node) in obj {
            let full = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
            let ty = node.get("type").and_then(Value::as_str);
            if let Some(sub) = node.get("properties") {
                if ty != Some("nested") {
                    walk(Some(sub), &full, out);
                }
                continue;
            }
            if matches!(ty, Some("text" | "keyword")) {
                out.push(full.clone());
            }
            if let Some(fields) = node.get("fields").and_then(Value::as_object) {
                for (sub, def) in fields {
                    if matches!(def.get("type").and_then(Value::as_str), Some("text" | "keyword")) {
                        out.push(format!("{full}.{sub}"));
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(mappings.get("properties"), "", &mut out);
    out
}

/// Term frequencies per (field, term) of a set of items.
fn term_freqs(
    items: &[Item],
    fields: &[String],
    mappings: &Value,
    analyzer: Option<&str>,
) -> HashMap<(String, String), u32> {
    let mut out: HashMap<(String, String), u32> = HashMap::new();
    for item in items {
        for f in fields {
            let toks: Vec<String> = match item {
                Item::Text(t) => match analyzer {
                    Some(a) => analysis::analyze(a, t),
                    None => analyze_for(mappings, f, t),
                },
                Item::Doc { fields: Some(only), .. } if !only.contains(f) => continue,
                Item::Doc { source, .. } => tokens_for(mappings, source, f),
                Item::Missing => continue,
            };
            for t in toks {
                *out.entry((f.clone(), t)).or_insert(0) += 1;
            }
        }
    }
    out
}

fn num(v: &Value, key: &str) -> Option<f64> {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

pub fn eval(v: &Value, mappings: &Value, docs: &[CommittedDoc]) -> Result<Scores, EsError> {
    let fields: Vec<String> = match v.get("fields").and_then(Value::as_array) {
        Some(a) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        None => default_fields(mappings),
    };
    let fail_unsupported =
        v.get("fail_on_unsupported_field").and_then(Value::as_bool).unwrap_or(true);
    let mut used = Vec::new();
    for f in fields {
        match resolve_field(mappings, &f).1.as_deref() {
            None => {}
            Some("text" | "keyword" | "match_only_text") => used.push(f),
            Some(_) if fail_unsupported => {
                return Err(EsError::shard_failure(
                    "illegal_argument_exception",
                    &format!("more_like_this only supports text/keyword fields: [{f}]"),
                ));
            }
            Some(_) => {}
        }
    }
    let like = items(v.get("like"), docs);
    if v.get("like").is_none() {
        return Err(EsError::parsing("more_like_this requires 'like' to be specified"));
    }
    let unlike = items(v.get("unlike"), docs);
    let analyzer = v.get("analyzer").and_then(Value::as_str);
    let min_tf = num(v, "min_term_freq").unwrap_or(2.0) as u32;
    let min_df = num(v, "min_doc_freq").unwrap_or(5.0) as u64;
    let max_df = num(v, "max_doc_freq").map_or(u64::MAX, |n| n as u64);
    let min_len = num(v, "min_word_length").unwrap_or(0.0) as usize;
    let max_len = num(v, "max_word_length").unwrap_or(0.0) as usize;
    let max_terms = num(v, "max_query_terms").unwrap_or(25.0) as usize;
    let boost_terms = num(v, "boost_terms").unwrap_or(0.0) as f32;
    let stop: HashSet<String> = v
        .get("stop_words")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_lowercase).collect())
        .unwrap_or_default();

    let skip: HashSet<(String, String)> =
        term_freqs(&unlike, &used, mappings, analyzer).into_keys().collect();
    let mut stats: HashMap<String, FieldTerms> = HashMap::new();
    for f in &used {
        stats.insert(f.clone(), FieldTerms::new(mappings, docs, f));
    }
    let num_docs = docs.len() as f64;
    // (score, field, term), best first.
    let mut queue: Vec<(f32, String, String)> = Vec::new();
    for ((field, term), tf) in term_freqs(&like, &used, mappings, analyzer) {
        let len = term.chars().count();
        if (min_len > 0 && len < min_len)
            || (max_len > 0 && len > max_len)
            || stop.contains(&term)
            || skip.contains(&(field.clone(), term.clone()))
            || (min_tf > 0 && tf < min_tf)
        {
            continue;
        }
        let df = stats[&field].doc_freq(&term);
        if (min_df > 0 && df < min_df) || df > max_df || df == 0 {
            continue;
        }
        // ClassicSimilarity's idf.
        let idf = ((num_docs + 1.0) / (df as f64 + 1.0)).ln() + 1.0;
        queue.push((tf as f32 * idf as f32, field, term));
    }
    queue.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| (&a.1, &a.2).cmp(&(&b.1, &b.2))));
    queue.truncate(max_terms);
    let best = queue.first().map_or(1.0, |q| q.0);
    let mut clauses: Vec<Scores> = Vec::new();
    for (score, field, term) in &queue {
        let ft = &stats[field];
        let b = if boost_terms > 0.0 { boost_terms * score / best } else { 1.0 };
        clauses.push(ft.term_scores(term, ft.doc_freq(term), b));
    }
    let msm = v.get("minimum_should_match").cloned().unwrap_or(Value::String("30%".into()));
    let required = min_should_match(&msm, clauses.len())?.max(1);
    let mut out = Scores::new();
    let mut hits: HashMap<usize, usize> = HashMap::new();
    for c in clauses {
        for (i, s) in c {
            *out.entry(i).or_insert(0.0) += s;
            *hits.entry(i).or_insert(0) += 1;
        }
    }
    out.retain(|i, _| hits[i] >= required);
    // The liked documents themselves are left out unless `include`.
    if !v.get("include").and_then(Value::as_bool).unwrap_or(false) {
        for item in &like {
            if let Item::Doc { id: Some(id), index, .. } = item {
                out.retain(|i, _| {
                    let d = &docs[*i];
                    !(d.id == *id && index.as_ref().is_none_or(|x| *x == d.index))
                });
            }
        }
    }
    let boost = num(v, "boost").unwrap_or(1.0) as f32;
    out.values_mut().for_each(|s| *s *= boost);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(id: &str, foo: &str) -> CommittedDoc {
        CommittedDoc {
            index: "i".into(),
            id: id.into(),
            source: json!({"foo": foo}),
            version: 1,
            seq: 0,
            full_source: None,
        }
    }

    #[test]
    fn unlike_terms_are_left_out() {
        let docs = vec![doc("1", "bar baz selected"), doc("2", "bar"), doc("3", "bar baz")];
        let m = json!({"properties": {"foo": {"type": "text"}}});
        let q = json!({"like": {"_id": "1"}, "unlike": {"_id": "3"}, "include": true,
                       "min_doc_freq": 0, "min_term_freq": 0});
        let hits: Vec<usize> = eval(&q, &m, &docs).unwrap().into_keys().collect();
        assert_eq!(hits, vec![0]);
    }

    #[test]
    fn default_min_term_freq_needs_repeated_terms() {
        let docs = vec![doc("1", "bar")];
        let m = json!({"properties": {"foo": {"type": "text"}}});
        let q = json!({"like": [{"_id": "1"}], "fields": ["foo"]});
        assert!(eval(&q, &m, &docs).unwrap().is_empty());
    }
}
