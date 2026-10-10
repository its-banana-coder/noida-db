//! `rescore`: re-scores the top `window_size` hits with a second query,
//! combined by `score_mode` (`total`, `multiply`, `avg`, `max`, `min`)
//! with `query_weight`/`rescore_query_weight`; hits below the window keep
//! their order, their score times `query_weight`. Several rescorers apply
//! in turn.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::Value;

use super::search::{CommittedDoc, EsError, eval};

/// One rescorer's effect on a hit, for its `_explanation`.
#[derive(Clone, Debug)]
pub struct Step {
    pub primary: f32,
    pub query_weight: f32,
    /// The rescore query's score, when it matched.
    pub secondary: Option<f32>,
    pub rescore_weight: f32,
    pub mode: String,
}

fn rescorers(spec: &Value) -> Vec<&Value> {
    match spec {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    }
}

fn combine(mode: &str, a: f32, b: f32) -> f32 {
    match mode {
        "multiply" => a * b,
        "avg" => (a + b) / 2.0,
        "max" => a.max(b),
        "min" => a.min(b),
        _ => a + b,
    }
}

/// Applies the request's `rescore` to `ranked` (sorted by score) in
/// place, returning each rescored hit's steps.
pub fn apply<K>(
    body: &Value,
    sorted: bool,
    ranked: &mut [(usize, f32, K)],
    mappings: &Value,
    docs: &[CommittedDoc],
) -> Result<HashMap<usize, Vec<Step>>, EsError> {
    let mut steps: HashMap<usize, Vec<Step>> = HashMap::new();
    let Some(spec) = body.get("rescore") else { return Ok(steps) };
    if sorted {
        return Err(EsError::shard_failure(
            "illegal_argument_exception",
            "Cannot use [sort] option in conjunction with [rescore].",
        ));
    }
    for r in rescorers(spec) {
        let window = r.get("window_size").and_then(Value::as_u64).unwrap_or(10) as usize;
        let Some(q) = r.get("query") else {
            return Err(EsError::parsing("missing rescore type"));
        };
        let rq = q
            .get("rescore_query")
            .ok_or_else(|| EsError::parsing("[query] rescore_query cannot be null"))?;
        let qw = q.get("query_weight").and_then(Value::as_f64).unwrap_or(1.0) as f32;
        let rw = q.get("rescore_query_weight").and_then(Value::as_f64).unwrap_or(1.0) as f32;
        let mode = q.get("score_mode").and_then(Value::as_str).unwrap_or("total").to_string();
        if !matches!(mode.as_str(), "total" | "multiply" | "avg" | "max" | "min") {
            return Err(EsError::new(
                400,
                "illegal_argument_exception",
                &format!("illegal score_mode [{mode}]"),
            ));
        }
        let secondary = eval(rq, mappings, docs)?;
        let window = window.min(ranked.len());
        for (pos, (idx, score, _)) in ranked.iter_mut().enumerate() {
            let primary = *score;
            let matched = if pos < window { secondary.get(idx).copied() } else { None };
            *score = match matched {
                Some(s) => combine(&mode, primary * qw, s * rw),
                None => primary * qw,
            };
            if pos < window {
                steps.entry(*idx).or_default().push(Step {
                    primary,
                    query_weight: qw,
                    secondary: matched,
                    rescore_weight: rw,
                    mode: mode.clone(),
                });
            }
        }
        ranked[..window]
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0)));
    }
    Ok(steps)
}
