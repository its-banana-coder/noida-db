//! `rescore`: the query rescorer. The top `window_size` hits (by score)
//! are scored again with `rescore_query` and the two scores combined
//! (`score_mode`, weighted by `query_weight`/`rescore_query_weight`);
//! hits below the window only get `query_weight` applied, and all of
//! them are re-sorted, as Elasticsearch's `QueryRescorer` does.

use serde_json::Value;
use std::cmp::Ordering;

use super::search::{CommittedDoc, EsError, eval};

struct Rescorer {
    window: usize,
    query: Value,
    query_weight: f32,
    rescore_weight: f32,
    mode: String,
}

fn parse_one(v: &Value) -> Result<Rescorer, EsError> {
    let window = v.get("window_size").and_then(Value::as_u64).unwrap_or(10) as usize;
    let q = v.get("query").ok_or_else(|| EsError::parsing("missing rescore type"))?;
    let query = q
        .get("rescore_query")
        .cloned()
        .ok_or_else(|| EsError::parsing("[query] rescore_query cannot be null"))?;
    let num = |k: &str| q.get(k).and_then(Value::as_f64).map(|f| f as f32);
    let mode = q.get("score_mode").and_then(Value::as_str).unwrap_or("total").to_string();
    if !matches!(mode.as_str(), "total" | "multiply" | "avg" | "max" | "min") {
        return Err(EsError::new(
            400,
            "illegal_argument_exception",
            &format!("illegal score_mode [{mode}]"),
        ));
    }
    Ok(Rescorer {
        window,
        query,
        query_weight: num("query_weight").unwrap_or(1.0),
        rescore_weight: num("rescore_query_weight").unwrap_or(1.0),
        mode,
    })
}

/// The request's rescorers (one object or an array of them), in order.
fn parse(body: &Value) -> Result<Vec<Rescorer>, EsError> {
    match body.get("rescore") {
        None | Some(Value::Null) => Ok(vec![]),
        Some(Value::Array(a)) => a.iter().map(parse_one).collect(),
        Some(o) => Ok(vec![parse_one(o)?]),
    }
}

/// Rescoring needs hits ordered by score: any other sort is refused.
pub fn check_sort(body: &Value, sorted_by_score: bool) -> Result<(), EsError> {
    if body.get("rescore").is_some_and(|r| !r.is_null()) && !sorted_by_score {
        return Err(EsError::shard_failure(
            "illegal_argument_exception",
            "Cannot use [sort] option in conjunction with [rescore].",
        ));
    }
    Ok(())
}

fn by_score(a: &(usize, f32), b: &(usize, f32)) -> Ordering {
    b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0))
}

/// Rescores `ranked` (doc position, score; best first) in place. `keep`
/// is how many hits the request returns (`from + size`): a shard hands
/// over at least that many, or the largest window, to be rescored.
/// Returns whether anything was rescored.
pub fn apply(
    body: &Value,
    ranked: &mut Vec<(usize, f32)>,
    mappings: &Value,
    docs: &[CommittedDoc],
    keep: usize,
) -> Result<bool, EsError> {
    let rescorers = parse(body)?;
    if rescorers.is_empty() {
        return Ok(false);
    }
    let n = rescorers.iter().map(|r| r.window).max().unwrap_or(0).max(keep).min(ranked.len());
    let mut top: Vec<(usize, f32)> = ranked[..n].to_vec();
    for r in &rescorers {
        let second = eval(&r.query, mappings, docs)?;
        let w = r.window.min(top.len());
        let mut window: Vec<(usize, f32)> = top[..w]
            .iter()
            .map(|&(i, s)| {
                let primary = s * r.query_weight;
                let score = match second.get(&i) {
                    None => primary,
                    Some(&s2) => {
                        let secondary = s2 * r.rescore_weight;
                        match r.mode.as_str() {
                            "multiply" => primary * secondary,
                            "avg" => (primary + secondary) / 2.0,
                            "max" => primary.max(secondary),
                            "min" => primary.min(secondary),
                            _ => primary + secondary,
                        }
                    }
                };
                (i, score)
            })
            .collect();
        window.sort_by(by_score);
        // Hits below the window are treated as not matching the rescore
        // query: only the query weight applies, then all are re-sorted.
        for h in top.iter_mut().skip(w) {
            h.1 *= r.query_weight;
        }
        top.splice(..w, window);
        top.sort_by(by_score);
    }
    ranked.splice(..n, top);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(id: &str) -> CommittedDoc {
        CommittedDoc {
            index: "i".into(),
            id: id.into(),
            source: json!({}),
            version: 1,
            seq: 0,
            full_source: None,
            tsid: None,
        }
    }

    #[test]
    fn window_hits_are_combined_and_the_rest_weighted() {
        let docs = vec![doc("1"), doc("2"), doc("3")];
        let body = json!({"rescore": {"window_size": 2, "query": {
            "rescore_query": {"match_all": {}}, "query_weight": 5, "rescore_query_weight": 10}}});
        let mut ranked = vec![(0, 1.0), (1, 1.0), (2, 1.0)];
        assert!(apply(&body, &mut ranked, &json!({}), &docs, 10).unwrap());
        assert_eq!(ranked, vec![(0, 15.0), (1, 15.0), (2, 5.0)]);
        assert!(check_sort(&body, false).is_err());
    }
}
