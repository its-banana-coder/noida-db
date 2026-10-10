//! Where each value sits in a raw JSON body, for the errors that cite a
//! `[line:col]` (or `"line"`/`"col"`) the way Elasticsearch's parser
//! reports them: a scalar at its first character, an object both at its
//! `{` (where parsing it starts) and at its `}` (where a field mapper that
//! skipped over it stands when it fails).

/// One value of the body, in document order.
#[derive(Debug, Clone, PartialEq)]
pub struct ValuePos {
    /// The dotted field path (`a.b`); array elements share their array's
    /// path, and the top-level value has an empty one.
    pub path: String,
    /// `(line, col)` of the value's first character, both 1-based.
    pub start: (usize, usize),
    /// `(line, col)` of its last character (`}` / `]` for a container).
    pub end: (usize, usize),
    pub kind: Kind,
    /// An element of an array (its path is the array's).
    pub in_array: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    Scalar,
    Object,
    Array,
}

impl ValuePos {
    /// The parser token a value starts with, as Elasticsearch names it.
    pub fn token(&self, raw_first: char) -> &'static str {
        match (self.kind, raw_first) {
            (Kind::Object, _) => "START_OBJECT",
            (Kind::Array, _) => "START_ARRAY",
            (_, '"') => "VALUE_STRING",
            (_, 't' | 'f') => "VALUE_BOOLEAN",
            (_, 'n') => "VALUE_NULL",
            _ => "VALUE_NUMBER",
        }
    }
}

struct Scanner<'a> {
    chars: Vec<char>,
    i: usize,
    line: usize,
    col: usize,
    out: &'a mut Vec<ValuePos>,
}

impl Scanner<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.i).copied()
    }

    fn bump(&mut self) {
        if let Some(c) = self.peek() {
            self.i += 1;
            if c == '\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
        }
    }

    fn ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }

    fn string(&mut self) -> Option<String> {
        if self.peek() != Some('"') {
            return None;
        }
        self.bump();
        let mut s = String::new();
        loop {
            let c = self.peek()?;
            self.bump();
            match c {
                '"' => return Some(s),
                '\\' => {
                    let e = self.peek()?;
                    self.bump();
                    match e {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        'r' => s.push('\r'),
                        'b' => s.push('\u{8}'),
                        'f' => s.push('\u{c}'),
                        'u' => {
                            let mut hex = String::new();
                            for _ in 0..4 {
                                hex.push(self.peek()?);
                                self.bump();
                            }
                            if let Some(ch) =
                                u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                            {
                                s.push(ch);
                            }
                        }
                        other => s.push(other),
                    }
                }
                other => s.push(other),
            }
        }
    }

    fn value(&mut self, path: &str, in_array: bool) -> Option<()> {
        self.ws();
        let start = (self.line, self.col);
        match self.peek()? {
            '{' => {
                let at = self.out.len();
                self.out.push(ValuePos {
                    path: path.into(),
                    start,
                    end: start,
                    kind: Kind::Object,
                    in_array,
                });
                self.bump();
                self.ws();
                if self.peek() == Some('}') {
                    self.out[at].end = (self.line, self.col);
                    self.bump();
                    return Some(());
                }
                loop {
                    self.ws();
                    let key = self.string()?;
                    self.ws();
                    if self.peek()? != ':' {
                        return None;
                    }
                    self.bump();
                    let child = if path.is_empty() { key } else { format!("{path}.{key}") };
                    self.value(&child, false)?;
                    self.ws();
                    match self.peek()? {
                        ',' => self.bump(),
                        '}' => {
                            self.out[at].end = (self.line, self.col);
                            self.bump();
                            return Some(());
                        }
                        _ => return None,
                    }
                }
            }
            '[' => {
                let at = self.out.len();
                self.out.push(ValuePos {
                    path: path.into(),
                    start,
                    end: start,
                    kind: Kind::Array,
                    in_array,
                });
                self.bump();
                self.ws();
                if self.peek() == Some(']') {
                    self.out[at].end = (self.line, self.col);
                    self.bump();
                    return Some(());
                }
                loop {
                    self.value(path, true)?;
                    self.ws();
                    match self.peek()? {
                        ',' => self.bump(),
                        ']' => {
                            self.out[at].end = (self.line, self.col);
                            self.bump();
                            return Some(());
                        }
                        _ => return None,
                    }
                }
            }
            '"' => {
                self.string()?;
                let end = (self.line, self.col.saturating_sub(1));
                self.out.push(ValuePos {
                    path: path.into(),
                    start,
                    end,
                    kind: Kind::Scalar,
                    in_array,
                });
                Some(())
            }
            _ => {
                let mut end = start;
                while self
                    .peek()
                    .is_some_and(|c| !matches!(c, ',' | '}' | ']') && !c.is_whitespace())
                {
                    end = (self.line, self.col);
                    self.bump();
                }
                self.out.push(ValuePos {
                    path: path.into(),
                    start,
                    end,
                    kind: Kind::Scalar,
                    in_array,
                });
                Some(())
            }
        }
    }
}

/// Every value of `raw` in document order (as far as it parses).
pub fn scan(raw: &str) -> Vec<ValuePos> {
    let mut out = Vec::new();
    let mut s = Scanner { chars: raw.chars().collect(), i: 0, line: 1, col: 1, out: &mut out };
    let _ = s.value("", false);
    out
}

/// The `n`-th value (0-based, arrays flattened) found at `path`.
pub fn nth<'a>(all: &'a [ValuePos], path: &str, n: usize) -> Option<&'a ValuePos> {
    all.iter().filter(|v| v.path == path && v.kind != Kind::Array).nth(n)
}

/// The first character of the value at `p` in `raw`.
pub fn first_char(raw: &str, p: &ValuePos) -> char {
    raw.lines().nth(p.start.0 - 1).and_then(|l| l.chars().nth(p.start.1 - 1)).unwrap_or(' ')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_of_scalars_and_objects() {
        let all = scan(r#"{"ip":"garbage","o":{"a":1},"n":  [1, "zz"]}"#);
        let ip = nth(&all, "ip", 0).unwrap();
        assert_eq!(ip.start, (1, 7));
        let o = nth(&all, "o", 0).unwrap();
        assert_eq!(o.kind, Kind::Object);
        assert_eq!(o.start, (1, 21));
        assert_eq!(o.end, (1, 27));
        assert_eq!(nth(&all, "n", 1).unwrap().start, (1, 39));
        let lines = scan("{\n  \"a\": 1\n}");
        assert_eq!(nth(&lines, "a", 0).unwrap().start, (2, 8));
    }
}
