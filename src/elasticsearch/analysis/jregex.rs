//! Java regular expressions (what Elasticsearch's pattern tokenizers and
//! filters take): run by `regex-lite` after translating the syntax
//! differences (POSIX `\p{..}` classes, `\Q..\E`, `\uXXXX`, Java flag
//! names), or by the backtracking matcher when the pattern needs what
//! `regex-lite` lacks (lookaround, backreferences, atomic groups,
//! possessive quantifiers). Java-style replacement strings (`$1`,
//! `${name}`) are expanded by hand.

use regex_lite::Regex;

use super::backtrack;

/// A POSIX/Unicode property class as the body of a character class.
fn property_class(name: &str) -> Option<&'static str> {
    Some(match name {
        "Lower" | "javaLowerCase" | "Ll" | "IsLowercase" | "IsLowerCase" => {
            "a-z\u{DF}-\u{F6}\u{F8}-\u{FF}"
        }
        "Upper" | "javaUpperCase" | "Lu" | "IsUppercase" | "IsUpperCase" => {
            "A-Z\u{C0}-\u{D6}\u{D8}-\u{DE}"
        }
        "ASCII" => "\\x00-\\x7F",
        "Alpha" | "L" | "IsL" | "IsLetter" | "IsAlphabetic" | "javaLetter" | "Letter" => {
            "a-zA-Z\u{AA}\u{B5}\u{BA}\u{C0}-\u{D6}\u{D8}-\u{F6}\u{F8}-\u{2C1}\u{370}-\u{3FF}\u{400}-\u{52F}\u{531}-\u{587}\u{5D0}-\u{5EA}\u{620}-\u{64A}\u{904}-\u{939}\u{3041}-\u{3096}\u{30A1}-\u{30FA}\u{4E00}-\u{9FFF}\u{AC00}-\u{D7A3}"
        }
        "Digit" | "Nd" | "N" | "IsDigit" => "0-9",
        "Alnum" => "a-zA-Z0-9",
        "Punct" | "P" | "IsPunctuation" => "!-/:-@\\[-`{-~",
        "Graph" => "!-~",
        "Print" => " -~",
        "Blank" => " \\t",
        "Cntrl" => "\\x00-\\x1F\\x7F",
        "XDigit" => "0-9a-fA-F",
        "Space" | "javaWhitespace" | "IsWhite_Space" | "IsWhiteSpace" | "Z" | "Zs" => {
            " \\t\\n\\x0B\\f\\r"
        }
        _ => return None,
    })
}

/// Translates a Java pattern into `regex-lite` syntax.
pub fn translate(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    let mut in_class = 0usize;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' if i + 1 < chars.len() => {
                let n = chars[i + 1];
                match n {
                    'p' | 'P' => {
                        let (name, len) = if chars.get(i + 2) == Some(&'{') {
                            let end =
                                chars[i + 3..].iter().position(|&c| c == '}').ok_or_else(|| {
                                    format!("Unclosed character family near index {i}\n{pattern}")
                                })?;
                            (chars[i + 3..i + 3 + end].iter().collect::<String>(), end + 4)
                        } else {
                            (chars.get(i + 2).map(|c| c.to_string()).unwrap_or_default(), 3)
                        };
                        let name = name
                            .strip_prefix("Is")
                            .filter(|n| property_class(n).is_some())
                            .unwrap_or(&name);
                        let body = property_class(name).ok_or_else(|| {
                            format!("Unknown character property name {{{name}}} near index {i}")
                        })?;
                        let neg = n == 'P';
                        if in_class > 0 {
                            if neg {
                                return Err(format!(
                                    "unsupported negated property class in [] near index {i}"
                                ));
                            }
                            out.push_str(body);
                        } else {
                            out.push('[');
                            if neg {
                                out.push('^');
                            }
                            out.push_str(body);
                            out.push(']');
                        }
                        i += len;
                        continue;
                    }
                    'Q' => {
                        let rest: String = chars[i + 2..].iter().collect();
                        let (lit, skip) = match rest.find("\\E") {
                            Some(p) => (rest[..p].to_string(), p + 2),
                            None => (rest.clone(), rest.len()),
                        };
                        out.push_str(&regex_lite::escape(&lit));
                        i += 2 + lit.chars().count() + (skip - lit.len());
                        continue;
                    }
                    'u' if chars.len() >= i + 6 => {
                        let hex: String = chars[i + 2..i + 6].iter().collect();
                        out.push_str(&format!("\\x{{{hex}}}"));
                        i += 6;
                        continue;
                    }
                    'h' => {
                        out.push_str(if in_class > 0 { " \\t\u{A0}" } else { "[ \\t\u{A0}]" });
                    }
                    'Z' => out.push_str("\\z"),
                    'e' => out.push_str("\\x1B"),
                    'a' => out.push_str("\\x07"),
                    'R' => out.push_str("(?:\\r\\n|[\\n\\r\\x0B\\x0C])"),
                    'G' => return Err("\\G is not supported".into()),
                    _ => {
                        out.push('\\');
                        out.push(n);
                    }
                }
                i += 2;
                continue;
            }
            '[' => {
                in_class += 1;
                out.push('[');
                // A leading `]` or `^]` is a literal.
                if chars.get(i + 1) == Some(&'^') {
                    out.push('^');
                    i += 1;
                }
                if chars.get(i + 1) == Some(&']') {
                    out.push_str("\\]");
                    i += 1;
                }
            }
            ']' if in_class > 0 => {
                in_class -= 1;
                out.push(']');
            }
            '(' if in_class == 0 && chars.get(i + 1) == Some(&'?') => {
                let next = chars.get(i + 2).copied();
                let next2 = chars.get(i + 3).copied();
                match (next, next2) {
                    (Some('='), _)
                    | (Some('!'), _)
                    | (Some('<'), Some('='))
                    | (Some('<'), Some('!')) => {
                        return Err("lookaround assertions are not supported".into());
                    }
                    (Some('>'), _) => {
                        out.push_str("(?:");
                        i += 3;
                        continue;
                    }
                    _ => out.push('('),
                }
            }
            '+' if in_class == 0
                && i > 0
                && matches!(chars[i - 1], '*' | '+' | '?' | '}')
                && !(i >= 2 && chars[i - 2] == '\\') =>
            {
                // A possessive quantifier: matched greedily instead.
            }
            _ => out.push(c),
        }
        i += 1;
    }
    Ok(out)
}

/// Java `Pattern` flag names (`CASE_INSENSITIVE|COMMENTS`) as an inline
/// flag group.
pub fn flags_prefix(flags: &str) -> String {
    let mut f = String::new();
    for name in flags.split('|').map(str::trim) {
        match name {
            "CASE_INSENSITIVE" => f.push('i'),
            "COMMENTS" => f.push('x'),
            "MULTILINE" => f.push('m'),
            "DOTALL" => f.push('s'),
            _ => {}
        }
    }
    if f.is_empty() { f } else { format!("(?{f})") }
}

/// Group spans of one match, in character offsets (group 0 = the match).
pub type Groups = Vec<Option<(usize, usize)>>;

/// A compiled Java pattern.
#[derive(Debug, Clone)]
pub struct JPattern {
    imp: Imp,
    names: Vec<(String, usize)>,
}

#[derive(Debug, Clone)]
enum Imp {
    Lite(Regex),
    Backtrack(Box<backtrack::Program>),
}

/// Whether `regex-lite` can't express the pattern.
fn needs_backtracking(pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' => {
                if chars.get(i + 1).is_some_and(|c| (c.is_ascii_digit() && *c != '0') || *c == 'k')
                {
                    return true;
                }
                i += 2;
                continue;
            }
            '(' if chars.get(i + 1) == Some(&'?')
                && matches!(chars.get(i + 2), Some('=') | Some('!') | Some('>') | Some('<'))
                && !(chars.get(i + 2) == Some(&'<')
                    && chars.get(i + 3).is_some_and(|c| c.is_ascii_alphabetic())) =>
            {
                return true;
            }
            '+' if i > 0 && matches!(chars[i - 1], '*' | '+' | '?' | '}') => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

impl JPattern {
    /// Compiles a Java pattern (with optional Java flag names).
    pub fn compile(pattern: &str, flags: &str) -> Result<JPattern, String> {
        if needs_backtracking(pattern) {
            let prog = backtrack::compile(pattern, flags)?;
            let names = prog.names.clone();
            return Ok(JPattern { imp: Imp::Backtrack(Box::new(prog)), names });
        }
        let translated = translate(pattern)?;
        let re = Regex::new(&format!("{}{translated}", flags_prefix(flags))).map_err(|e| {
            let msg = e.to_string();
            format!("{}\n{pattern}", msg.lines().last().unwrap_or(&msg).trim())
        })?;
        let names = re
            .capture_names()
            .enumerate()
            .filter_map(|(i, n)| n.map(|n| (n.to_string(), i)))
            .collect();
        Ok(JPattern { imp: Imp::Lite(re), names })
    }

    /// Every non-overlapping match of `text`, with its groups.
    pub fn captures_all(&self, text: &[char]) -> Vec<Groups> {
        match &self.imp {
            Imp::Backtrack(p) => p.captures_all(text),
            Imp::Lite(re) => {
                let s: String = text.iter().collect();
                let mut b2c = vec![0usize; s.len() + 1];
                let mut ci = 0;
                for (b, c) in s.char_indices() {
                    for k in 0..c.len_utf8() {
                        b2c[b + k] = ci;
                    }
                    ci += 1;
                }
                b2c[s.len()] = ci;
                re.captures_iter(&s)
                    .map(|caps| {
                        (0..caps.len())
                            .map(|g| caps.get(g).map(|m| (b2c[m.start()], b2c[m.end()])))
                            .collect()
                    })
                    .collect()
            }
        }
    }

    /// Whether all of `text` matches.
    pub fn full_match(&self, text: &str) -> bool {
        match &self.imp {
            Imp::Backtrack(p) => p.full_match(&text.chars().collect::<Vec<_>>()),
            Imp::Lite(re) => re.find(text).is_some_and(|m| m.start() == 0 && m.end() == text.len()),
        }
    }

    /// Expands a Java replacement string (`$1`, `${name}`, `\$`) for a
    /// match of `text`.
    pub fn expand(&self, replacement: &str, groups: &Groups, text: &[char]) -> String {
        let group = |g: usize| -> String {
            groups
                .get(g)
                .copied()
                .flatten()
                .map(|(a, b)| text[a..b].iter().collect())
                .unwrap_or_default()
        };
        let chars: Vec<char> = replacement.chars().collect();
        let mut out = String::new();
        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                '\\' if i + 1 < chars.len() => {
                    out.push(chars[i + 1]);
                    i += 2;
                }
                '$' if chars.get(i + 1) == Some(&'{') => {
                    match chars[i + 2..].iter().position(|&c| c == '}').map(|p| p + i + 2) {
                        Some(end) => {
                            let name: String = chars[i + 2..end].iter().collect();
                            if let Some((_, g)) = self.names.iter().find(|(n, _)| *n == name) {
                                out.push_str(&group(*g));
                            }
                            i = end + 1;
                        }
                        None => {
                            out.push('$');
                            i += 1;
                        }
                    }
                }
                '$' if chars.get(i + 1).is_some_and(char::is_ascii_digit) => {
                    // Java takes as many digits as still name a group.
                    let mut j = i + 1;
                    let mut g = 0usize;
                    while j < chars.len() && chars[j].is_ascii_digit() {
                        let next = g * 10 + chars[j].to_digit(10).unwrap_or(0) as usize;
                        if j > i + 1 && next >= groups.len() {
                            break;
                        }
                        g = next;
                        j += 1;
                    }
                    out.push_str(&group(g));
                    i = j;
                }
                c => {
                    out.push(c);
                    i += 1;
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replace_all(p: &JPattern, text: &str, rep: &str) -> String {
        let chars: Vec<char> = text.chars().collect();
        let mut out = String::new();
        let mut last = 0;
        for g in p.captures_all(&chars) {
            let (a, b) = g[0].unwrap();
            out.extend(&chars[last..a]);
            out.push_str(&p.expand(rep, &g, &chars));
            last = b;
        }
        out.extend(&chars[last..]);
        out
    }

    #[test]
    fn posix_classes_lookaround_and_replacements() {
        let p = JPattern::compile("(\\p{Lower})(\\p{Upper})", "").unwrap();
        assert_eq!(replace_all(&p, "fooBar", "$1 $2"), "foo Bar");
        let p = JPattern::compile("(?<=\\p{Lower})(?=\\p{Upper})", "").unwrap();
        assert_eq!(replace_all(&p, "fooBarBaz", " "), "foo Bar Baz");
        let p = JPattern::compile("(\\d+)-(?=\\d)", "").unwrap();
        assert_eq!(replace_all(&p, "123-456-789", "$1_"), "123_456_789");
        assert!(JPattern::compile("ABC", "CASE_INSENSITIVE").unwrap().full_match("abc"));
    }
}
