//! The dialect-neutral pieces of noida-db's SQL engine, shared by the Postgres
//! and MySQL services: exact numerics, calendar arithmetic, time zones and
//! JSON. Nothing here knows about a wire protocol, a SQLSTATE or a catalog.
//!
//! Dialect rules (parsing, error codes, type names, output formats) stay in
//! `src/postgres/` and `src/mysql/`.

pub mod datetime;
pub mod hash;
pub mod json;
pub mod numeric;
pub mod tz;

/// Byte ranges of `( ... )` groups that open with `(SELECT`/`((`, hold a
/// set operator at their own level, and so are a set operation in
/// parentheses: each becomes `(SELECT * FROM (...) AS noida_setop)`.
pub fn rewrite_paren_setops(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    // Depth and kind of each byte: inside a literal/identifier or not.
    let mut code = vec![true; b.len()];
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => {
                let start = i;
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                for c in &mut code[start..=i.min(b.len() - 1)] {
                    *c = false;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                let start = i;
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                for c in &mut code[start..i] {
                    *c = false;
                }
            }
            _ => {}
        }
        i += 1;
    }
    let word_at = |i: usize, w: &str| {
        let end = i + w.len();
        end <= b.len()
            && sql[i..end].eq_ignore_ascii_case(w)
            && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
            && (end == b.len() || !(b[end].is_ascii_alphanumeric() || b[end] == b'_'))
    };
    let skip_ws = |mut j: usize| {
        while j < b.len() && b[j].is_ascii_whitespace() {
            j += 1;
        }
        j
    };
    let mut spans: Vec<(usize, usize)> = vec![];
    for open in 0..b.len() {
        if !code[open] || b[open] != b'(' {
            continue;
        }
        // The group must start with another `(` (a parenthesised SELECT).
        let inner = skip_ws(open + 1);
        if inner >= b.len() || b[inner] != b'(' {
            continue;
        }
        let first = skip_ws(inner + 1);
        if !(word_at(first, "select") || word_at(first, "with") || word_at(first, "values")) {
            continue;
        }
        // Walk to the matching `)`, noting a set operator at depth 1.
        let (mut depth, mut j, mut setop) = (0i32, open, false);
        while j < b.len() {
            if code[j] {
                match b[j] {
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ if depth == 1
                        && (word_at(j, "union")
                            || word_at(j, "intersect")
                            || word_at(j, "except")) =>
                    {
                        setop = true
                    }
                    _ => {}
                }
            }
            j += 1;
        }
        if setop && j < b.len() && !spans.iter().any(|&(o, c)| open > o && j < c) {
            // A FROM-clause group (`FROM ((...) UNION (...)) s`) already
            // parses; only rewrite groups in expression position.
            let before = sql[..open].trim_end();
            let derived = before.len() >= 4
                && before[before.len() - 4..].eq_ignore_ascii_case("from")
                || before.ends_with(',') && before.to_ascii_lowercase().contains(" from ");
            if !derived {
                spans.push((open, j));
            }
        }
    }
    if spans.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + spans.len() * 40);
    let mut last = 0;
    for (open, close) in spans {
        out.push_str(&sql[last..open]);
        out.push_str("(SELECT * FROM ");
        out.push_str(&sql[open..=close]);
        out.push_str(" AS noida_setop)");
        last = close + 1;
    }
    out.push_str(&sql[last..]);
    Some(out)
}
