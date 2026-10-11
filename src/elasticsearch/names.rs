//! Date math in index and alias names: `<logs-{now/d}>` names the index
//! `logs-2024.01.15` (today, in the default `uuuu.MM.dd` format),
//! `<logs-{now/M{yyyy-MM}}>` formats the date, `{now/d{yyyy.MM.dd|+12:00}}`
//! reads it in a time zone, and `\{` / `\}` are literal braces.

use super::dates;

fn invalid(name: &str, why: &str) -> String {
    format!("invalid dynamic name expression [{}]. {why}", name.strip_prefix('<').unwrap_or(name))
}

/// The name a `<...>` date math expression resolves to; any other name as
/// given.
pub(crate) fn resolve(name: &str) -> Result<String, String> {
    resolve_at(name, dates::now_ms())
}

/// [`resolve`] with `now` at the given epoch millis.
fn resolve_at(name: &str, now: i64) -> Result<String, String> {
    let Some(inner) = name.strip_prefix('<').and_then(|n| n.strip_suffix('>')) else {
        return Ok(name.to_string());
    };
    let chars: Vec<char> = inner.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' if i + 1 < chars.len() => {
                out.push(chars[i + 1]);
                i += 2;
            }
            '}' => return Err(invalid(name, &format!("invalid character at position [{i}]."))),
            '{' => {
                // `{expr}` or `{expr{format|time_zone}}`.
                let start = i + 1;
                let mut j = start;
                while j < chars.len() && chars[j] != '{' && chars[j] != '}' {
                    j += 1;
                }
                let expr: String = chars[start..j].iter().collect();
                let mut format = "uuuu.MM.dd".to_string();
                let mut tz = 0;
                if chars.get(j) == Some(&'{') {
                    let fs = j + 1;
                    let mut k = fs;
                    while k < chars.len() && chars[k] != '}' {
                        k += 1;
                    }
                    let spec: String = chars[fs..k].iter().collect();
                    let (f, zone) = match spec.split_once('|') {
                        Some((f, z)) => (f, Some(z)),
                        None => (spec.as_str(), None),
                    };
                    if !f.is_empty() {
                        format = f.to_string();
                    }
                    if let Some(z) = zone {
                        tz = dates::parse_offset(z.trim())
                            .ok_or_else(|| invalid(name, &format!("unknown time zone [{z}]")))?;
                    }
                    j = k + 1;
                }
                if chars.get(j) != Some(&'}') {
                    return Err(invalid(name, "date math placeholder is open ended"));
                }
                // An explicit date (`2022-12-31||/d`) is read in the format.
                let ms =
                    dates::parse_math(&expr, now, false, Some(&format), tz).ok_or_else(|| {
                        let date = expr.split("||").next().unwrap_or(&expr);
                        let why =
                            format!("failed to parse date field [{date}] with format [{format}]");
                        format!("{why}: [{why}]")
                    })?;
                out.push_str(&dates::format(ms, Some(&format), tz));
                i = j + 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(out)
}

/// A comma-separated index expression with each date math part resolved.
pub(crate) fn resolve_list(expr: &str) -> Result<String, String> {
    if !expr.contains('<') {
        return Ok(expr.to_string());
    }
    let parts: Result<Vec<String>, String> = expr.split(',').map(resolve).collect();
    Ok(parts?.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_date_math_names() {
        assert_eq!(
            resolve("<logs_http_{2022-12-31||/d{yyyy-MM-dd}}>").unwrap(),
            "logs_http_2022-12-31"
        );
        assert_eq!(resolve("<logs-{2022.12.31||/M}>").unwrap(), "logs-2022.12.01");
        assert_eq!(resolve("<a\\{b\\}-{2022.01.02}>").unwrap(), "a{b}-2022.01.02");
        assert!(resolve("<logs-{2022-12-31||/M}>").is_err());
        assert_eq!(resolve("plain").unwrap(), "plain");
        assert!(resolve("<bad-{now/d>").is_err());
        // 2024-03-15T10:00:00Z
        let now = 1_710_496_800_000;
        assert_eq!(resolve_at("<logstash-{now/M}>", now).unwrap(), "logstash-2024.03.01");
        assert_eq!(resolve_at("<a-{now/d{yyyy.MM}}>", now).unwrap(), "a-2024.03");
        assert_eq!(resolve_at("<x\\{y\\}-{now/d}>", now).unwrap(), "x{y}-2024.03.15");
    }
}
