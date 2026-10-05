//! `UPDATE t SET arr[i] = v`: sqlparser doesn't parse a subscripted
//! assignment target, so before parsing it becomes
//! `arr = array_set_element(arr, (i), (v))`, which does what Postgres's
//! array element assignment does.

/// Rewrites every `col[idx] = value` in an UPDATE's (or ON CONFLICT DO
/// UPDATE's) SET list; `None` when there's nothing to rewrite.
pub fn rewrite(sql: &str) -> Option<String> {
    if !sql.contains('[') || !sql.to_ascii_lowercase().contains("set") {
        return None;
    }
    let c: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len() + 32);
    let mut i = 0;
    let mut changed = false;
    let mut last_kw = String::new();
    while i < c.len() {
        // Copy quoted text and comments verbatim.
        if let Some(end) = skip_quoted(&c, i) {
            out.extend(&c[i..end]);
            i = end;
            continue;
        }
        if c[i].is_alphabetic() && (i == 0 || !is_word(c[i - 1])) {
            let start = i;
            while i < c.len() && is_word(c[i]) {
                i += 1;
            }
            let word: String = c[start..i].iter().collect::<String>().to_ascii_lowercase();
            out.extend(&c[start..i]);
            if word == "set" && last_kw == "update" {
                let (text, n, did) = rewrite_set_list(&c, i);
                out.push_str(&text);
                i = n;
                changed |= did;
            }
            if matches!(word.as_str(), "update" | "select" | "insert" | "delete") {
                last_kw = word;
            }
            continue;
        }
        if c[i] == ';' {
            last_kw.clear();
        }
        out.push(c[i]);
        i += 1;
    }
    changed.then_some(out)
}

fn is_word(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$'
}

/// The end of a string literal, quoted identifier, dollar-quoted body or
/// comment starting at `i`, if one does.
fn skip_quoted(c: &[char], i: usize) -> Option<usize> {
    match c[i] {
        '\'' | '"' => {
            let q = c[i];
            let mut j = i + 1;
            while j < c.len() {
                if c[j] == q {
                    if c.get(j + 1) == Some(&q) {
                        j += 2;
                        continue;
                    }
                    return Some(j + 1);
                }
                j += 1;
            }
            Some(c.len())
        }
        '-' if c.get(i + 1) == Some(&'-') => {
            Some(c[i..].iter().position(|&x| x == '\n').map_or(c.len(), |p| i + p))
        }
        '$' => {
            let mut j = i + 1;
            while j < c.len() && (c[j].is_alphanumeric() || c[j] == '_') {
                j += 1;
            }
            if c.get(j) != Some(&'$') {
                return None;
            }
            let tag: String = c[i..=j].iter().collect();
            let rest: String = c[j + 1..].iter().collect();
            let close = rest.find(&tag)?;
            Some(j + 1 + rest[..close].chars().count() + tag.chars().count())
        }
        _ => None,
    }
}

/// Scans an expression from `i` up to (not including) a top-level `stop`
/// character or keyword; returns its end.
fn scan_expr(c: &[char], mut i: usize, stop_at_comma: bool) -> usize {
    let mut depth = 0i32;
    while i < c.len() {
        if let Some(end) = skip_quoted(c, i) {
            i = end;
            continue;
        }
        match c[i] {
            '(' | '[' => depth += 1,
            ')' | ']' if depth == 0 => return i,
            ')' | ']' => depth -= 1,
            ',' if depth == 0 && stop_at_comma => return i,
            ';' if depth == 0 => return i,
            ch if depth == 0 && ch.is_alphabetic() && (i == 0 || !is_word(c[i - 1])) => {
                let mut j = i;
                while j < c.len() && is_word(c[j]) {
                    j += 1;
                }
                let w: String = c[i..j].iter().collect::<String>().to_ascii_lowercase();
                if matches!(w.as_str(), "where" | "from" | "returning") {
                    return i;
                }
                i = j;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    i
}

/// Rewrites one SET list starting at `i` (just after `SET`).
fn rewrite_set_list(c: &[char], mut i: usize) -> (String, usize, bool) {
    let mut out = String::new();
    let mut changed = false;
    loop {
        // Leading whitespace.
        while i < c.len() && c[i].is_whitespace() {
            out.push(c[i]);
            i += 1;
        }
        // Target: an identifier, possibly quoted or dotted.
        let t0 = i;
        while i < c.len() {
            if c[i] == '"' {
                i = skip_quoted(c, i).unwrap_or(c.len());
            } else if is_word(c[i]) || c[i] == '.' {
                i += 1;
            } else {
                break;
            }
        }
        let target: String = c[t0..i].iter().collect();
        let mut j = i;
        while j < c.len() && c[j].is_whitespace() {
            j += 1;
        }
        if !target.is_empty() && c.get(j) == Some(&'[') {
            let idx_end = scan_expr(c, j + 1, false);
            let mut k = idx_end + 1;
            while k < c.len() && c[k].is_whitespace() {
                k += 1;
            }
            if c.get(idx_end) == Some(&']') && c.get(k) == Some(&'=') && c.get(k + 1) != Some(&'>')
            {
                let idx: String = c[j + 1..idx_end].iter().collect();
                let vend = scan_expr(c, k + 1, true);
                let val: String = c[k + 1..vend].iter().collect();
                // The column name alone (a dotted target keeps its last part).
                let col = target.rsplit('.').next().unwrap_or(&target);
                out.push_str(&format!(
                    "{col} = array_set_element({col}, ({}), ({}))",
                    idx.trim(),
                    val.trim()
                ));
                if val.ends_with(char::is_whitespace) {
                    out.push(' ');
                }
                changed = true;
                i = vend;
            } else {
                out.push_str(&target);
                let vend = scan_expr(c, i, true);
                out.extend(&c[i..vend]);
                i = vend;
            }
        } else {
            out.push_str(&target);
            let vend = scan_expr(c, i, true);
            out.extend(&c[i..vend]);
            i = vend;
        }
        if c.get(i) == Some(&',') {
            out.push(',');
            i += 1;
            continue;
        }
        return (out, i, changed);
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite;

    #[test]
    fn rewrites_subscripted_targets_only() {
        assert_eq!(
            rewrite("UPDATE t SET a[2] = 'z', b = 1 WHERE id = 2").unwrap(),
            "UPDATE t SET a = array_set_element(a, (2), ('z')), b = 1 WHERE id = 2"
        );
        assert_eq!(
            rewrite("UPDATE t SET a[i + 1] = f(x, y) RETURNING a").unwrap(),
            "UPDATE t SET a = array_set_element(a, (i + 1), (f(x, y))) RETURNING a"
        );
        assert!(rewrite("UPDATE t SET a = '[x]'").is_none());
        assert!(rewrite("SELECT a[1] FROM t").is_none());
    }
}
