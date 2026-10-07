//! Glob matching with Redis's exact semantics (a port of `stringmatchlen`),
//! used by KEYS, SCAN MATCH, PSUBSCRIBE and friends.

pub fn matches(pattern: &[u8], s: &[u8], nocase: bool) -> bool {
    match_at(pattern, s, nocase, &mut false, 0)
}

/// Key matching as KEYS/SCAN do it: a bare `*` matches everything, even the
/// empty key, without running the matcher.
pub fn matches_key(pattern: &[u8], key: &[u8]) -> bool {
    pattern == b"*" || matches(pattern, key, false)
}

fn eq(a: u8, b: u8, nocase: bool) -> bool {
    if nocase { a.eq_ignore_ascii_case(&b) } else { a == b }
}

/// `skip_longer` is Redis's `skipLongerMatches`: once the rest of the
/// pattern after a `*` matched nowhere in the remaining string, no earlier
/// `*` can help by matching more, so the whole search stops. Without it,
/// patterns like `a*a*a*...b` take exponential time.
fn match_at(pat: &[u8], s: &[u8], nocase: bool, skip_longer: &mut bool, nesting: usize) -> bool {
    // Redis caps recursion to protect against pathological patterns.
    if nesting > 1000 {
        return false;
    }
    let (mut p, mut i) = (0, 0);
    while p < pat.len() && i < s.len() {
        match pat[p] {
            b'*' => {
                while p + 1 < pat.len() && pat[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pat.len() {
                    return true;
                }
                for k in i..s.len() {
                    if match_at(&pat[p + 1..], &s[k..], nocase, skip_longer, nesting + 1) {
                        return true;
                    }
                    if *skip_longer {
                        return false;
                    }
                }
                *skip_longer = true;
                return false;
            }
            b'?' => i += 1,
            b'[' => {
                p += 1;
                let not = pat.get(p) == Some(&b'^');
                if not {
                    p += 1;
                }
                let mut matched = false;
                loop {
                    match pat.get(p) {
                        None => {
                            // Unterminated class: step back so the outer
                            // loop's increment lands on the end.
                            p -= 1;
                            break;
                        }
                        Some(b'\\') if p + 1 < pat.len() => {
                            p += 1;
                            if pat[p] == s[i] {
                                matched = true;
                            }
                        }
                        Some(b']') => break,
                        Some(_) if p + 2 < pat.len() && pat[p + 1] == b'-' => {
                            let (mut start, mut end, mut c) = (pat[p], pat[p + 2], s[i]);
                            if start > end {
                                std::mem::swap(&mut start, &mut end);
                            }
                            if nocase {
                                start = start.to_ascii_lowercase();
                                end = end.to_ascii_lowercase();
                                c = c.to_ascii_lowercase();
                            }
                            p += 2;
                            if (start..=end).contains(&c) {
                                matched = true;
                            }
                        }
                        Some(&c) => {
                            if eq(c, s[i], nocase) {
                                matched = true;
                            }
                        }
                    }
                    p += 1;
                }
                if not {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                i += 1;
            }
            c => {
                let c = if c == b'\\' && p + 1 < pat.len() {
                    p += 1;
                    pat[p]
                } else {
                    c
                };
                if !eq(c, s[i], nocase) {
                    return false;
                }
                i += 1;
            }
        }
        p += 1;
        if i == s.len() {
            while pat.get(p) == Some(&b'*') {
                p += 1;
            }
            break;
        }
    }
    p >= pat.len() && i == s.len()
}

#[cfg(test)]
mod tests {
    use super::{matches, matches_key};

    fn m(p: &str, s: &str) -> bool {
        matches(p.as_bytes(), s.as_bytes(), false)
    }

    #[test]
    fn pathological_patterns_finish() {
        let key = "a".repeat(40);
        assert!(!m("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b", &key));
        let long = "a".repeat(50000);
        let pat = "*".to_string() + &"a*".repeat(50000) + "b";
        assert!(!m(&pat, &long));
        assert!(m("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*", &key));
    }

    #[test]
    fn literals_and_wildcards() {
        assert!(m("hello", "hello"));
        assert!(!m("hello", "hello!"));
        // Redis quirk: callers special-case a bare "*" (see `matches_key`).
        assert!(!m("*", ""));
        assert!(matches_key(b"*", b""));
        assert!(m("*", "anything"));
        assert!(m("h*o", "hello"));
        assert!(m("h**o", "ho"));
        assert!(m("h?llo", "hallo"));
        assert!(!m("h?llo", "hllo"));
        assert!(m("a*b*c", "aXXbYYc"));
        assert!(!m("a*b*c", "aXXbYY"));
        assert!(m("abc*", "abc"));
        assert!(!m("", "a"));
        assert!(m("", ""));
    }

    #[test]
    fn classes() {
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-c]llo", "hbllo"));
        assert!(m("h[c-a]llo", "hbllo"));
        assert!(m("[\\]]", "]"));
    }

    #[test]
    fn escapes_and_case() {
        assert!(m("h\\*llo", "h*llo"));
        assert!(!m("h\\*llo", "hello"));
        assert!(matches(b"HeLLo", b"hello", true));
        assert!(!matches(b"HeLLo", b"hello", false));
    }
}
