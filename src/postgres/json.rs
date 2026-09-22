//! JSON values. `jsonb` is stored parsed (keys sorted and de-duplicated the
//! way Postgres does); `json` is stored as the original text.

use std::cmp::Ordering;

use super::numeric::{Numeric, cmp_num};

#[derive(Clone, Debug)]
pub enum Json {
    Null,
    Bool(bool),
    Num(Numeric),
    Str(String),
    Array(Vec<Json>),
    /// Insertion order for `json`; jsonb order after [`Json::normalize`].
    Object(Vec<(String, Json)>),
}

#[derive(Debug)]
pub struct JsonErr(pub String);

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn err<T>(&self, what: &str) -> Result<T, JsonErr> {
        Err(JsonErr(what.to_string()))
    }

    fn value(&mut self) -> Result<Json, JsonErr> {
        self.ws();
        let Some(&c) = self.s.get(self.i) else {
            return self.err("The input string ended unexpectedly.");
        };
        match c {
            b'{' => {
                self.depth += 1;
                if self.depth > 6400 {
                    return self.err("stack depth limit exceeded");
                }
                self.i += 1;
                let mut members = vec![];
                self.ws();
                if self.s.get(self.i) == Some(&b'}') {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(Json::Object(members));
                }
                loop {
                    self.ws();
                    if self.s.get(self.i) != Some(&b'"') {
                        return self.err("Expected string or \"}\", but found something else.");
                    }
                    let k = self.string()?;
                    self.ws();
                    if self.s.get(self.i) != Some(&b':') {
                        return self.err("Expected \":\", but found something else.");
                    }
                    self.i += 1;
                    let v = self.value()?;
                    members.push((k, v));
                    self.ws();
                    match self.s.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            break;
                        }
                        _ => return self.err("Expected \",\" or \"}\", but found something else."),
                    }
                }
                self.depth -= 1;
                Ok(Json::Object(members))
            }
            b'[' => {
                self.depth += 1;
                if self.depth > 6400 {
                    return self.err("stack depth limit exceeded");
                }
                self.i += 1;
                let mut items = vec![];
                self.ws();
                if self.s.get(self.i) == Some(&b']') {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(Json::Array(items));
                }
                loop {
                    items.push(self.value()?);
                    self.ws();
                    match self.s.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            break;
                        }
                        _ => return self.err("Expected \",\" or \"]\", but found something else."),
                    }
                }
                self.depth -= 1;
                Ok(Json::Array(items))
            }
            b'"' => Ok(Json::Str(self.string()?)),
            b't' => self.word("true", Json::Bool(true)),
            b'f' => self.word("false", Json::Bool(false)),
            b'n' => self.word("null", Json::Null),
            b'-' | b'0'..=b'9' => self.number(),
            _ => self.err("Token is invalid."),
        }
    }

    fn word(&mut self, w: &str, v: Json) -> Result<Json, JsonErr> {
        if self.s[self.i..].starts_with(w.as_bytes()) {
            let end = self.i + w.len();
            if self.s.get(end).is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_') {
                return self.err("Token is invalid.");
            }
            self.i = end;
            Ok(v)
        } else {
            self.err("Token is invalid.")
        }
    }

    fn number(&mut self) -> Result<Json, JsonErr> {
        let start = self.i;
        if self.s[self.i] == b'-' {
            self.i += 1;
        }
        let int_start = self.i;
        while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
            self.i += 1;
        }
        let int_len = self.i - int_start;
        if int_len == 0 || (int_len > 1 && self.s[int_start] == b'0') {
            return self.err("Token is invalid.");
        }
        if self.s.get(self.i) == Some(&b'.') {
            self.i += 1;
            let f = self.i;
            while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
                self.i += 1;
            }
            if f == self.i {
                return self.err("Token is invalid.");
            }
        }
        if matches!(self.s.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.s.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            let f = self.i;
            while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
                self.i += 1;
            }
            if f == self.i {
                return self.err("Token is invalid.");
            }
        }
        if self.s.get(self.i).is_some_and(|c| c.is_ascii_alphanumeric()) {
            return self.err("Token is invalid.");
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).unwrap();
        Numeric::parse(text).map(Json::Num).map_err(|_| JsonErr("Token is invalid.".into()))
    }

    fn string(&mut self) -> Result<String, JsonErr> {
        self.i += 1; // opening quote
        let mut out = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.i) else {
                return self.err("The input string ended unexpectedly.");
            };
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return self.err("The input string ended unexpectedly.");
                    };
                    self.i += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                if self.s.get(self.i..self.i + 2) != Some(b"\\u") {
                                    return self.err(
                                        "Unicode low surrogate must follow a high surrogate.",
                                    );
                                }
                                self.i += 2;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return self.err(
                                        "Unicode low surrogate must follow a high surrogate.",
                                    );
                                }
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            } else if (0xDC00..0xE000).contains(&cp) {
                                return self
                                    .err("Unicode low surrogate must follow a high surrogate.");
                            }
                            if cp == 0 {
                                return Err(JsonErr("\\u0000 cannot be converted to text.".into()));
                            }
                            let ch = char::from_u32(cp).unwrap_or('\u{FFFD}');
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return self.err("Escape sequence is invalid."),
                    }
                }
                0..=0x1f => return self.err("Character with value 0x0a must be escaped."),
                _ => out.push(c),
            }
        }
        String::from_utf8(out).map_err(|_| JsonErr("invalid UTF-8".into()))
    }

    fn hex4(&mut self) -> Result<u32, JsonErr> {
        let h = self
            .s
            .get(self.i..self.i + 4)
            .ok_or(JsonErr("\"\\u\" must be followed by four hexadecimal digits.".into()))?;
        let s = std::str::from_utf8(h).map_err(|_| JsonErr("bad escape".into()))?;
        let v = u32::from_str_radix(s, 16)
            .map_err(|_| JsonErr("\"\\u\" must be followed by four hexadecimal digits.".into()))?;
        self.i += 4;
        Ok(v)
    }
}

/// Parses JSON text. Object members keep input order and duplicates.
pub fn parse(s: &str) -> Result<Json, JsonErr> {
    let mut p = Parser { s: s.as_bytes(), i: 0, depth: 0 };
    let v = p.value()?;
    p.ws();
    if p.i != p.s.len() {
        return Err(JsonErr("Expected end of input, but found something else.".into()));
    }
    Ok(v)
}

/// Parses as `jsonb`: sorted, de-duplicated keys.
pub fn parse_jsonb(s: &str) -> Result<Json, JsonErr> {
    parse(s).map(Json::normalize)
}

/// jsonb key order: shorter keys first, then bytewise.
pub fn key_cmp(a: &str, b: &str) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

impl Json {
    /// Applies jsonb's object rules recursively (last duplicate wins).
    pub fn normalize(self) -> Json {
        match self {
            Json::Array(items) => Json::Array(items.into_iter().map(Json::normalize).collect()),
            Json::Object(members) => {
                let mut out: Vec<(String, Json)> = Vec::with_capacity(members.len());
                for (k, v) in members {
                    let v = v.normalize();
                    match out.binary_search_by(|(ek, _)| key_cmp(ek, &k)) {
                        Ok(i) => out[i].1 = v,
                        Err(i) => out.insert(i, (k, v)),
                    }
                }
                Json::Object(out)
            }
            other => other,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Json::Null => "null",
            Json::Bool(_) => "boolean",
            Json::Num(_) => "number",
            Json::Str(_) => "string",
            Json::Array(_) => "array",
            Json::Object(_) => "object",
        }
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            // For json with duplicates, the last one wins, as in Postgres.
            Json::Object(m) => m.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn index(&self, i: i64) -> Option<&Json> {
        match self {
            Json::Array(a) => {
                let idx = if i < 0 { a.len() as i64 + i } else { i };
                if idx < 0 { None } else { a.get(idx as usize) }
            }
            _ => None,
        }
    }

    /// Follows a `#>` path.
    pub fn path(&self, path: &[Option<String>]) -> Option<&Json> {
        let mut cur = self;
        for step in path {
            let step = step.as_ref()?;
            cur = match cur {
                Json::Object(_) => cur.get(step)?,
                Json::Array(_) => cur.index(step.trim().parse().ok()?)?,
                _ => return None,
            };
        }
        Some(cur)
    }

    /// `->>`-style text: strings unquoted, null as SQL NULL.
    pub fn as_text(&self, jsonb: bool) -> Option<String> {
        match self {
            Json::Null => None,
            Json::Str(s) => Some(s.clone()),
            other => Some(if jsonb { other.to_jsonb_string() } else { other.to_compact_string() }),
        }
    }

    /// jsonb's canonical text: `{"a": 1, "b": [1, 2]}`.
    pub fn to_jsonb_string(&self) -> String {
        let mut s = String::new();
        self.write(&mut s, ", ", ": ");
        s
    }

    /// Without spaces, as `row_to_json`/`json_agg` elements print.
    pub fn to_compact_string(&self) -> String {
        let mut s = String::new();
        self.write(&mut s, ",", ":");
        s
    }

    pub fn write(&self, out: &mut String, comma: &str, colon: &str) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Num(n) => out.push_str(&n.to_string()),
            Json::Str(s) => escape_into(out, s),
            Json::Array(a) => {
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push_str(comma);
                    }
                    v.write(out, comma, colon);
                }
                out.push(']');
            }
            Json::Object(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push_str(comma);
                    }
                    escape_into(out, k);
                    out.push_str(colon);
                    v.write(out, comma, colon);
                }
                out.push('}');
            }
        }
    }

    /// `jsonb_pretty`.
    pub fn pretty(&self) -> String {
        let mut s = String::new();
        self.pretty_into(&mut s, 0);
        s
    }

    fn pretty_into(&self, out: &mut String, level: usize) {
        let pad = |out: &mut String, l: usize| out.push_str(&"    ".repeat(l));
        match self {
            Json::Array(a) if !a.is_empty() => {
                out.push_str("[\n");
                for (i, v) in a.iter().enumerate() {
                    pad(out, level + 1);
                    v.pretty_into(out, level + 1);
                    if i + 1 < a.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                pad(out, level);
                out.push(']');
            }
            Json::Object(m) if !m.is_empty() => {
                out.push_str("{\n");
                for (i, (k, v)) in m.iter().enumerate() {
                    pad(out, level + 1);
                    escape_into(out, k);
                    out.push_str(": ");
                    v.pretty_into(out, level + 1);
                    if i + 1 < m.len() {
                        out.push(',');
                    }
                    out.push('\n');
                }
                pad(out, level);
                out.push('}');
            }
            other => other.write(out, ", ", ": "),
        }
    }

    /// jsonb `@>`.
    pub fn contains(&self, other: &Json) -> bool {
        match (self, other) {
            (Json::Object(a), Json::Object(b)) => {
                b.iter().all(|(k, bv)| a.iter().any(|(ak, av)| ak == k && av.contains_value(bv)))
            }
            (Json::Array(a), Json::Array(b)) => {
                b.iter().all(|bv| a.iter().any(|av| av.contains_value(bv)))
            }
            // A top-level array contains a primitive it holds.
            (Json::Array(a), b) if !matches!(b, Json::Object(_)) => a.iter().any(|av| av == b),
            (a, b) => a == b,
        }
    }

    fn contains_value(&self, other: &Json) -> bool {
        match (self, other) {
            (Json::Object(_), Json::Object(_)) | (Json::Array(_), Json::Array(_)) => {
                self.contains(other)
            }
            (a, b) => a == b,
        }
    }

    /// jsonb `?`: key (or array string element) exists.
    pub fn has_key(&self, k: &str) -> bool {
        match self {
            Json::Object(m) => m.iter().any(|(key, _)| key == k),
            Json::Array(a) => a.iter().any(|v| matches!(v, Json::Str(s) if s == k)),
            Json::Str(s) => s == k,
            _ => false,
        }
    }

    /// Rank for jsonb ordering between different types.
    fn rank(&self) -> u8 {
        match self {
            Json::Null => 1,
            Json::Str(_) => 2,
            Json::Num(_) => 3,
            Json::Bool(_) => 4,
            Json::Array(_) => 5,
            Json::Object(_) => 6,
        }
    }
}

impl PartialEq for Json {
    fn eq(&self, o: &Json) -> bool {
        cmp_jsonb(self, o) == Ordering::Equal
    }
}

/// jsonb btree ordering: Object > Array > Boolean > Number > String > Null,
/// arrays/objects first by size.
pub fn cmp_jsonb(a: &Json, b: &Json) -> Ordering {
    // An empty top-level array sorts below null in Postgres; rarely matters.
    match (a, b) {
        (Json::Null, Json::Null) => Ordering::Equal,
        (Json::Bool(x), Json::Bool(y)) => x.cmp(y),
        (Json::Num(x), Json::Num(y)) => cmp_num(x, y),
        (Json::Str(x), Json::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Json::Array(x), Json::Array(y)) => x.len().cmp(&y.len()).then_with(|| {
            for (p, q) in x.iter().zip(y) {
                let c = cmp_jsonb(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            Ordering::Equal
        }),
        (Json::Object(x), Json::Object(y)) => x.len().cmp(&y.len()).then_with(|| {
            for ((ka, va), (kb, vb)) in x.iter().zip(y) {
                let c = key_cmp(ka, kb).then_with(|| cmp_jsonb(va, vb));
                if c != Ordering::Equal {
                    return c;
                }
            }
            Ordering::Equal
        }),
        _ => a.rank().cmp(&b.rank()),
    }
}

/// Postgres's `escape_json`.
pub fn escape_into(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    escape_into(&mut o, s);
    o
}

/// Returns the raw source text of each top-level member of a json object
/// or array, preserving the input's formatting (as `json ->` does).
pub fn raw_children(s: &str) -> Option<Vec<(Option<String>, &str)>> {
    let mut p = Parser { s: s.as_bytes(), i: 0, depth: 0 };
    p.ws();
    let open = *p.s.get(p.i)?;
    if open != b'{' && open != b'[' {
        return None;
    }
    p.i += 1;
    let mut out = vec![];
    p.ws();
    let close = if open == b'{' { b'}' } else { b']' };
    if p.s.get(p.i) == Some(&close) {
        return Some(out);
    }
    loop {
        p.ws();
        let key = if open == b'{' {
            let k = p.string().ok()?;
            p.ws();
            if p.s.get(p.i) != Some(&b':') {
                return None;
            }
            p.i += 1;
            p.ws();
            Some(k)
        } else {
            None
        };
        p.ws();
        let start = p.i;
        p.value().ok()?;
        out.push((key, &s[start..p.i]));
        p.ws();
        match p.s.get(p.i) {
            Some(b',') => p.i += 1,
            Some(c) if *c == close => break,
            _ => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_normalizes() {
        let j =
            parse_jsonb(r#"{"bb": 1, "a": [1, 2.50, "x"], "bb": {"z": null}, "c": true}"#).unwrap();
        assert_eq!(j.to_jsonb_string(), r#"{"a": [1, 2.50, "x"], "c": true, "bb": {"z": null}}"#);
    }

    #[test]
    fn rejects_bad_json() {
        for s in ["", "{", "[1,]", "01", "1.", "tru", "{\"a\" 1}", "\"\\x\"", "nan", "[1] x"] {
            assert!(parse(s).is_err(), "{s}");
        }
        assert!(parse(" [1, {\"a\": \"\\u00e9\\ud83d\\ude00\"}] ").is_ok());
    }

    #[test]
    fn containment_and_keys() {
        let a = parse_jsonb(r#"{"a": 1, "b": [1, 2, 3], "c": {"d": "e"}}"#).unwrap();
        assert!(a.contains(&parse_jsonb(r#"{"b": [3, 1]}"#).unwrap()));
        assert!(a.contains(&parse_jsonb(r#"{"c": {}}"#).unwrap()));
        assert!(!a.contains(&parse_jsonb(r#"{"a": 2}"#).unwrap()));
        assert!(a.has_key("c"));
        assert!(parse_jsonb(r#"["a", "b"]"#).unwrap().has_key("a"));
    }

    #[test]
    fn raw_members() {
        let r = raw_children(r#"{"a": [1,  2], "b" : "x"}"#).unwrap();
        assert_eq!(r[0], (Some("a".into()), "[1,  2]"));
        assert_eq!(r[1], (Some("b".into()), "\"x\""));
    }
}
