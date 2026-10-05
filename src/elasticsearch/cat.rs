//! `_cat` tables: the same rows rendered as Elasticsearch's aligned text
//! (`?v` for a header, numeric columns right-aligned) or as JSON
//! (`?format=json`), with `?h=` choosing columns.

use serde_json::{Map, Value, json};
use std::collections::HashMap;

/// A rendered `_cat` response: JSON rows, or text for the server to send
/// as `text/plain` (wrapped so the dispatcher's JSON plumbing can carry
/// it).
pub fn render(
    columns: &[&str],
    numeric: &[&str],
    rows: Vec<Vec<String>>,
    q: &HashMap<String, String>,
) -> Value {
    let wanted: Vec<usize> = match q.get("h") {
        Some(h) => {
            h.split(',').filter_map(|c| columns.iter().position(|col| *col == c.trim())).collect()
        }
        None => (0..columns.len()).collect(),
    };
    if q.get("format").map(String::as_str) == Some("json") {
        return Value::Array(
            rows.iter()
                .map(|r| {
                    let m: Map<String, Value> = wanted
                        .iter()
                        .map(|&i| (columns[i].to_string(), json!(r[i].clone())))
                        .collect();
                    Value::Object(m)
                })
                .collect(),
        );
    }
    let header = q.contains_key("v");
    let mut widths: Vec<usize> =
        wanted.iter().map(|&i| if header { columns[i].len() } else { 0 }).collect();
    for r in &rows {
        for (k, &i) in wanted.iter().enumerate() {
            widths[k] = widths[k].max(r[i].chars().count());
        }
    }
    let line = |cells: Vec<(&str, bool)>| -> String {
        let n = cells.len();
        let mut out = String::new();
        for (k, (cell, right)) in cells.into_iter().enumerate() {
            let pad = widths[k].saturating_sub(cell.chars().count());
            if right {
                out.push_str(&" ".repeat(pad));
                out.push_str(cell);
            } else {
                out.push_str(cell);
                if k + 1 < n {
                    out.push_str(&" ".repeat(pad));
                }
            }
            if k + 1 < n {
                out.push(' ');
            }
        }
        out.trim_end().to_string() + "\n"
    };
    let mut text = String::new();
    if header {
        text.push_str(&line(
            wanted.iter().map(|&i| (columns[i], numeric.contains(&columns[i]))).collect(),
        ));
    }
    for r in &rows {
        text.push_str(&line(
            wanted.iter().map(|&i| (r[i].as_str(), numeric.contains(&columns[i]))).collect(),
        ));
    }
    json!({ RAW_TEXT: text })
}

/// The key a response body uses to say "send this string as text/plain".
pub const RAW_TEXT: &str = "\u{0}noida_raw_text";

/// Bytes the way `_cat` prints them (`7.6kb`, `1mb`, `512b`).
pub fn human_bytes(n: u64) -> String {
    let units = ["b", "kb", "mb", "gb", "tb"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < units.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n}b")
    } else {
        let s = format!("{v:.1}");
        format!("{}{}", s.trim_end_matches(".0"), units[u])
    }
}

/// `epoch` and `timestamp` (HH:MM:SS, UTC) columns.
pub fn now_columns() -> (String, String) {
    let ms = super::dates::now_ms();
    let secs = ms / 1000;
    let t = secs.rem_euclid(86_400);
    (secs.to_string(), format!("{:02}:{:02}:{:02}", t / 3600, t / 60 % 60, t % 60))
}
