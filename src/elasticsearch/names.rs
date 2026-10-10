//! Date math in index and alias names: `<logs-{now/d}>` names the index
//! `logs-2024.03.15`. Inside `<...>`, each `{math}` or
//! `{math{format|time_zone}}` is replaced by the evaluated date (default
//! format `uuuu.MM.dd`, UTC); `\{` and `\}` are literal braces.

use super::dates;

/// `name` with its date math resolved (unchanged when it has none).
pub fn resolve(name: &str) -> String {
    let Some(inner) = name.strip_prefix('<').and_then(|n| n.strip_suffix('>')) else {
        return name.to_string();
    };
    resolve_at(inner, dates::now_ms()).unwrap_or_else(|| name.to_string())
}

fn resolve_at(inner: &str, now: i64) -> Option<String> {
    let chars: Vec<char> = inner.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' if matches!(chars.get(i + 1), Some('{' | '}')) => {
                out.push(chars[i + 1]);
                i += 2;
            }
            '{' => {
                // Up to the matching close brace (one nested `{format}`).
                let mut depth = 0;
                let start = i + 1;
                let mut end = None;
                for (j, c) in chars.iter().enumerate().skip(i) {
                    match c {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                end = Some(j);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let end = end?;
                let expr: String = chars[start..end].iter().collect();
                out.push_str(&evaluate(&expr, now)?);
                i = end + 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Some(out)
}

/// `math` or `math{format|time_zone}` as the formatted date.
fn evaluate(expr: &str, now: i64) -> Option<String> {
    let (math, spec) = match expr.find('{') {
        Some(p) => (&expr[..p], expr[p + 1..].strip_suffix('}')?),
        None => (expr, ""),
    };
    let (format, zone) = match spec.split_once('|') {
        Some((f, z)) => (f, Some(z)),
        None => (spec, None),
    };
    let format = if format.is_empty() { "uuuu.MM.dd" } else { format };
    let tz = match zone {
        Some(z) => dates::parse_offset(z).unwrap_or(0),
        None => 0,
    };
    let ms = dates::parse_math(math, now, false, None, tz)?;
    Some(dates::format(ms, Some(format), tz))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_math_names() {
        assert_eq!(resolve("plain"), "plain");
        assert_eq!(resolve("<logs_http_{2022-12-31||/d{yyyy-MM-dd}}>"), "logs_http_2022-12-31");
        // 2024-03-15T10:00:00Z
        let now = 1_710_496_800_000;
        assert_eq!(resolve_at("logstash-{now/M}", now).unwrap(), "logstash-2024.03.01");
        assert_eq!(resolve_at("a-{now/d{yyyy.MM}}", now).unwrap(), "a-2024.03");
        assert_eq!(resolve_at("x\\{y\\}-{now/d}", now).unwrap(), "x{y}-2024.03.15");
    }
}
