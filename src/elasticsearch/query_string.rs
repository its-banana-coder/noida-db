//! `query_string` (Lucene's classic query syntax) and `simple_query_string`
//! (its forgiving cousin), compiled into ordinary Query DSL that
//! `search::eval` already knows how to run.
//!
//! `query_string`: `field:term`, `field:(a OR b)`, `"phrase"~slop`, `+`/`-`,
//! `AND`/`OR`/`NOT` (`&&`/`||`/`!`), wildcards, `term~fuzz`, `^boost`,
//! ranges (`[a TO b]`, `{a TO b}`, `>`/`>=`/`<`/`<=`), `_exists_:field`,
//! grouping, escapes. A term across several fields is a `dis_max` (the
//! `best_fields` type); a syntax error is a `query_shard_exception`.
//!
//! `simple_query_string`: `+` (AND), `|` (OR), `-` (NOT), `"phrase"~N`,
//! `prefix*`, `term~N`, parentheses; never fails (a stray operator is
//! text). A term across several fields sums them (`most_fields`).

use serde_json::{Value, json};

use super::search::{EsError, resolve_field};

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Term(String),
    Phrase(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
    Plus,
    Minus,
    Colon,
    Caret(f64),
    Tilde(Option<f64>),
    Range { lower: String, upper: String, incl_lo: bool, incl_hi: bool },
    Cmp(String),
}

fn lex(q: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = q.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let special = |c: char| "()[]{}\"^~:+-!&|\\/".contains(c) || c.is_whitespace();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ':' => {
                out.push(Tok::Colon);
                i += 1;
                // `field:>=10` style comparisons.
                if i < chars.len() && (chars[i] == '>' || chars[i] == '<') {
                    let mut op = chars[i].to_string();
                    i += 1;
                    if i < chars.len() && chars[i] == '=' {
                        op.push('=');
                        i += 1;
                    }
                    out.push(Tok::Cmp(op));
                }
            }
            '+' => {
                out.push(Tok::Plus);
                i += 1;
            }
            '-' => {
                out.push(Tok::Minus);
                i += 1;
            }
            '!' => {
                out.push(Tok::Not);
                i += 1;
            }
            '&' if chars.get(i + 1) == Some(&'&') => {
                out.push(Tok::And);
                i += 2;
            }
            '|' if chars.get(i + 1) == Some(&'|') => {
                out.push(Tok::Or);
                i += 2;
            }
            '^' => {
                i += 1;
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let n: f64 = chars[start..i]
                    .iter()
                    .collect::<String>()
                    .parse()
                    .map_err(|_| "Cannot parse boost".to_string())?;
                out.push(Tok::Caret(n));
            }
            '~' => {
                i += 1;
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                out.push(Tok::Tilde(s.parse().ok()));
            }
            '"' => {
                i += 1;
                let mut s = String::new();
                loop {
                    match chars.get(i) {
                        None => return Err("Lexical error: unterminated quote".to_string()),
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some('\\') => {
                            if let Some(n) = chars.get(i + 1) {
                                s.push(*n);
                            }
                            i += 2;
                        }
                        Some(ch) => {
                            s.push(*ch);
                            i += 1;
                        }
                    }
                }
                out.push(Tok::Phrase(s));
            }
            '[' | '{' => {
                let incl_lo = c == '[';
                let close = chars[i..].iter().position(|ch| *ch == ']' || *ch == '}');
                let Some(close) = close.map(|p| p + i) else {
                    return Err("Cannot parse range: missing closing bracket".to_string());
                };
                let inner: String = chars[i + 1..close].iter().collect();
                let parts: Vec<&str> = inner.split(" TO ").collect();
                if parts.len() != 2 {
                    return Err(format!("Cannot parse range [{inner}]"));
                }
                out.push(Tok::Range {
                    lower: parts[0].trim().trim_matches('"').to_string(),
                    upper: parts[1].trim().trim_matches('"').to_string(),
                    incl_lo,
                    incl_hi: chars[close] == ']',
                });
                i = close + 1;
            }
            ']' | '}' => return Err(format!("Encountered \"{c}\"")),
            _ => {
                let mut s = String::new();
                while i < chars.len() && !special(chars[i]) {
                    s.push(chars[i]);
                    i += 1;
                }
                while i < chars.len() && chars[i] == '\\' {
                    if let Some(n) = chars.get(i + 1) {
                        s.push(*n);
                    }
                    i += 2;
                    while i < chars.len() && !special(chars[i]) {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if s.is_empty() {
                    i += 1;
                    continue;
                }
                out.push(match s.as_str() {
                    "AND" => Tok::And,
                    "OR" => Tok::Or,
                    "NOT" => Tok::Not,
                    _ => Tok::Term(s),
                });
            }
        }
    }
    Ok(out)
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Occur {
    Must,
    Should,
    MustNot,
}

/// Where unqualified terms go and how they're combined.
struct Ctx<'a> {
    mappings: &'a Value,
    fields: Vec<(String, f64)>,
    and_default: bool,
    /// Query terms on non-text fields that can't be parsed are skipped
    /// rather than failing (implied when searching `*`).
    lenient: bool,
    /// `best_fields` (dis_max) vs `most_fields` (sum) across fields.
    dis_max: bool,
    analyze_wildcard: bool,
    fuzziness_default: Value,
    phrase_slop: i64,
}

impl Ctx<'_> {
    /// One leaf query on every target field.
    fn leaf(&self, field: Option<&str>, make: &dyn Fn(&str, &str) -> Option<Value>) -> Value {
        let fields: Vec<(String, f64)> = match field {
            Some(f) => expand(self.mappings, f).into_iter().map(|f| (f, 1.0)).collect(),
            None => self.fields.clone(),
        };
        let mut qs: Vec<Value> = fields
            .iter()
            .filter_map(|(f, boost)| {
                let ty = resolve_field(self.mappings, f).1.unwrap_or_else(|| "text".into());
                let q = make(f, &ty)?;
                Some(if (*boost - 1.0).abs() > f64::EPSILON {
                    json!({"bool": {"must": [q], "boost": boost}})
                } else {
                    q
                })
            })
            .collect();
        match qs.len() {
            0 => json!({"match_none": {}}),
            1 => qs.pop().unwrap(),
            _ if self.dis_max => json!({"dis_max": {"queries": qs}}),
            _ => json!({"bool": {"should": qs}}),
        }
    }

    fn term_query(&self, field: Option<&str>, text: &str, fuzzy: Option<Option<f64>>) -> Value {
        let lenient = self.lenient;
        let fuzz = self.fuzziness_default.clone();
        self.leaf(field, &|f, ty| {
            if let Some(fz) = fuzzy {
                let fuzziness = fz.map_or_else(|| fuzz.clone(), |n| json!(n as i64));
                return Some(json!({"fuzzy": {f: {"value": text.to_lowercase(), "fuzziness": fuzziness}}}));
            }
            if text.contains('*') || text.contains('?') {
                let pattern = if ty == "keyword" { text.to_string() } else { text.to_lowercase() };
                if text == "*" {
                    return Some(json!({"exists": {"field": f}}));
                }
                if let Some(prefix) = pattern.strip_suffix('*')
                    && !prefix.contains('*')
                    && !prefix.contains('?')
                {
                    return Some(json!({"prefix": {f: prefix}}));
                }
                return Some(json!({"wildcard": {f: pattern}}));
            }
            match ty {
                "text" | "match_only_text" => Some(json!({"match": {f: {"query": text}}})),
                "keyword" | "constant_keyword" | "wildcard" | "flattened" => {
                    Some(json!({"term": {f: text}}))
                }
                "boolean" => match text {
                    "true" | "false" => Some(json!({"term": {f: text}})),
                    _ if lenient => None,
                    _ => Some(json!({"__error": format!("Can't parse boolean value [{text}]")})),
                },
                "date" | "date_nanos" => {
                    if lenient && super::dates::parse(text, None, 0).is_none() {
                        None
                    } else {
                        Some(json!({"range": {f: {"gte": text, "lte": text}}}))
                    }
                }
                _ => {
                    if text.parse::<f64>().is_ok() {
                        Some(json!({"term": {f: text.parse::<f64>().unwrap()}}))
                    } else if lenient {
                        None
                    } else {
                        Some(json!({"__error": format!("failed to create query: For input string: \"{text}\"")}))
                    }
                }
            }
        })
    }

    fn phrase_query(&self, field: Option<&str>, text: &str, slop: i64) -> Value {
        self.leaf(field, &|f, ty| match ty {
            "text" | "match_only_text" => {
                Some(json!({"match_phrase": {f: {"query": text, "slop": slop}}}))
            }
            "keyword" | "flattened" => Some(json!({"term": {f: text}})),
            // A quoted date or number is still one term of its type.
            "date" | "date_nanos" => Some(json!({"range": {f: {"gte": text, "lte": text}}})),
            _ => text.parse::<f64>().ok().map(|n| json!({"term": {f: n}})),
        })
    }
}

/// A field pattern (`title`, `name.*`, `*`) as concrete mapped fields.
fn expand(mappings: &Value, pattern: &str) -> Vec<String> {
    if !pattern.contains('*') {
        return vec![pattern.to_string()];
    }
    let mut all = Vec::new();
    collect_fields(mappings.get("properties"), "", &mut all);
    all.into_iter()
        .filter(|f| {
            let re = format!("^{}$", regex_lite::escape(pattern).replace("\\*", ".*"));
            regex_lite::Regex::new(&re).is_ok_and(|r| r.is_match(f))
        })
        .collect()
}

fn collect_fields(props: Option<&Value>, prefix: &str, out: &mut Vec<String>) {
    let Some(obj) = props.and_then(Value::as_object) else { return };
    for (name, node) in obj {
        let full = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
        if let Some(sub) = node.get("properties") {
            if node.get("type").and_then(Value::as_str) != Some("nested") {
                collect_fields(Some(sub), &full, out);
            }
            continue;
        }
        out.push(full.clone());
        if let Some(fields) = node.get("fields").and_then(Value::as_object) {
            for sub in fields.keys() {
                out.push(format!("{full}.{sub}"));
            }
        }
    }
}

fn parse_fields(spec: &Value, default: &[(String, f64)], mappings: &Value) -> Vec<(String, f64)> {
    let mut out = Vec::new();
    let list: Vec<String> = match spec.get("fields").and_then(Value::as_array) {
        Some(a) => a.iter().filter_map(Value::as_str).map(str::to_string).collect(),
        None => match spec.get("default_field").and_then(Value::as_str) {
            Some(f) => vec![f.to_string()],
            None => return default.to_vec(),
        },
    };
    for f in list {
        let (name, boost) = match f.split_once('^') {
            Some((n, b)) => (n.to_string(), b.parse().unwrap_or(1.0)),
            None => (f, 1.0),
        };
        for e in expand(mappings, &name) {
            out.push((e, boost));
        }
    }
    out
}

fn all_fields(mappings: &Value) -> Vec<(String, f64)> {
    expand(mappings, "*").into_iter().map(|f| (f, 1.0)).collect()
}

// --- query_string --------------------------------------------------------

struct Parser<'a> {
    toks: Vec<Tok>,
    pos: usize,
    ctx: &'a Ctx<'a>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    /// A sequence of clauses up to `)` or the end, combined the way
    /// Lucene's classic parser does (`AND` turns its neighbours into MUST,
    /// `OR` keeps them SHOULD, modifiers win).
    fn query(&mut self, field: Option<&str>) -> Result<Value, String> {
        let mut clauses: Vec<(Occur, Value)> = Vec::new();
        let mut conj: Option<Tok> = None;
        loop {
            match self.peek() {
                None | Some(Tok::RParen) => break,
                Some(Tok::And) | Some(Tok::Or) => {
                    let t = self.next().unwrap();
                    if clauses.is_empty() {
                        return Err(format!(
                            "Encountered \"{}\" at start",
                            if t == Tok::And { "AND" } else { "OR" }
                        ));
                    }
                    conj = Some(t);
                    continue;
                }
                _ => {}
            }
            let mut modifier = None;
            match self.peek() {
                Some(Tok::Plus) => {
                    self.next();
                    modifier = Some(Occur::Must);
                }
                Some(Tok::Minus) | Some(Tok::Not) => {
                    self.next();
                    modifier = Some(Occur::MustNot);
                }
                _ => {}
            }
            let clause = self.clause(field)?;
            // Lucene's QueryParserBase.addClause.
            if conj == Some(Tok::And)
                && let Some(last) = clauses.last_mut()
                && last.0 == Occur::Should
            {
                last.0 = Occur::Must;
            }
            if self.ctx.and_default
                && conj == Some(Tok::Or)
                && let Some(last) = clauses.last_mut()
                && last.0 == Occur::Must
            {
                last.0 = Occur::Should;
            }
            let occur = match modifier {
                Some(m) => m,
                None if self.ctx.and_default => {
                    if conj == Some(Tok::Or) {
                        Occur::Should
                    } else {
                        Occur::Must
                    }
                }
                None => {
                    if conj == Some(Tok::And) {
                        Occur::Must
                    } else {
                        Occur::Should
                    }
                }
            };
            clauses.push((occur, clause));
            conj = None;
        }
        if conj.is_some() {
            return Err("Encountered \"<EOF>\"".to_string());
        }
        Ok(combine(clauses))
    }

    fn clause(&mut self, field: Option<&str>) -> Result<Value, String> {
        // `field:` prefix?
        let mut field = field.map(str::to_string);
        if let (Some(Tok::Term(t)), Some(Tok::Colon)) =
            (self.toks.get(self.pos), self.toks.get(self.pos + 1))
        {
            field = Some(t.clone());
            self.pos += 2;
        }
        let tok = self.next().ok_or("Encountered \"<EOF>\"")?;
        let q = match tok {
            Tok::LParen => {
                let inner = self.query(field.as_deref())?;
                if self.next() != Some(Tok::RParen) {
                    return Err("Encountered \"<EOF>\": was expecting \")\"".to_string());
                }
                inner
            }
            Tok::Phrase(p) => {
                let slop = match self.peek() {
                    Some(Tok::Tilde(n)) => {
                        let n = n.unwrap_or(0.0) as i64;
                        self.next();
                        n
                    }
                    _ => self.ctx.phrase_slop,
                };
                self.ctx.phrase_query(field.as_deref(), &p, slop)
            }
            Tok::Term(t) => {
                if field.as_deref() == Some("_exists_") {
                    json!({"exists": {"field": t}})
                } else {
                    let fuzzy = match self.peek() {
                        Some(Tok::Tilde(n)) => {
                            let n = *n;
                            self.next();
                            Some(n)
                        }
                        _ => None,
                    };
                    self.ctx.term_query(field.as_deref(), &t, fuzzy)
                }
            }
            Tok::Range { lower, upper, incl_lo, incl_hi } => {
                let f = field.clone().ok_or("range needs a field")?;
                let mut cond = serde_json::Map::new();
                if lower != "*" {
                    cond.insert(if incl_lo { "gte" } else { "gt" }.into(), range_value(&lower));
                }
                if upper != "*" {
                    cond.insert(if incl_hi { "lte" } else { "lt" }.into(), range_value(&upper));
                }
                json!({"range": {f: cond}})
            }
            Tok::Cmp(op) => {
                let f = field.clone().ok_or("comparison needs a field")?;
                let Some(Tok::Term(v)) = self.next() else {
                    return Err(format!("Cannot parse '{op}'"));
                };
                let key = match op.as_str() {
                    ">" => "gt",
                    ">=" => "gte",
                    "<" => "lt",
                    _ => "lte",
                };
                json!({"range": {f: {key: range_value(&v)}}})
            }
            other => return Err(format!("Encountered \"{other:?}\"")),
        };
        if let Some(Tok::Caret(b)) = self.peek() {
            let b = *b;
            self.next();
            return Ok(json!({"bool": {"must": [q], "boost": b}}));
        }
        Ok(q)
    }
}

fn range_value(s: &str) -> Value {
    s.parse::<i64>()
        .map(|n| json!(n))
        .or_else(|_| s.parse::<f64>().map(|f| json!(f)))
        .unwrap_or_else(|_| json!(s))
}

/// A bool query from occur-tagged clauses; a purely negative one matches
/// everything else (Elasticsearch adds `match_all`).
fn combine(clauses: Vec<(Occur, Value)>) -> Value {
    if clauses.len() == 1 && clauses[0].0 != Occur::MustNot {
        return clauses.into_iter().next().unwrap().1;
    }
    let pick = |o: Occur| -> Vec<Value> {
        clauses.iter().filter(|c| c.0 == o).map(|c| c.1.clone()).collect()
    };
    let (must, should, must_not) = (pick(Occur::Must), pick(Occur::Should), pick(Occur::MustNot));
    let mut b = serde_json::Map::new();
    if must.is_empty() && should.is_empty() {
        b.insert("must".into(), json!([{"match_all": {}}]));
    }
    if !must.is_empty() {
        b.insert("must".into(), Value::Array(must));
    }
    if !should.is_empty() {
        b.insert("should".into(), Value::Array(should));
    }
    if !must_not.is_empty() {
        b.insert("must_not".into(), Value::Array(must_not));
    }
    json!({"bool": b})
}

fn find_error(v: &Value) -> Option<String> {
    match v {
        Value::Object(o) => {
            if let Some(e) = o.get("__error").and_then(Value::as_str) {
                return Some(e.to_string());
            }
            o.values().find_map(find_error)
        }
        Value::Array(a) => a.iter().find_map(find_error),
        _ => None,
    }
}

pub fn query_string(spec: &Value, mappings: &Value) -> Result<Value, EsError> {
    let text = spec
        .get("query")
        .and_then(Value::as_str)
        .ok_or_else(|| EsError::parsing("[query_string] requires 'query' field"))?;
    let explicit = spec.get("fields").is_some()
        || spec.get("default_field").is_some_and(|f| f.as_str() != Some("*"));
    let fields = parse_fields(spec, &all_fields(mappings), mappings);
    let ctx = Ctx {
        mappings,
        fields,
        and_default: spec
            .get("default_operator")
            .and_then(Value::as_str)
            .is_some_and(|o| o.eq_ignore_ascii_case("and")),
        lenient: spec.get("lenient").and_then(Value::as_bool).unwrap_or(!explicit),
        dis_max: spec.get("type").and_then(Value::as_str) != Some("most_fields"),
        analyze_wildcard: spec.get("analyze_wildcard").and_then(Value::as_bool).unwrap_or(false),
        fuzziness_default: spec.get("fuzziness").cloned().unwrap_or(json!("AUTO")),
        phrase_slop: spec.get("phrase_slop").and_then(Value::as_i64).unwrap_or(0),
    };
    let _ = ctx.analyze_wildcard;
    let fail = |m: String| {
        EsError::shard_failure(
            "query_shard_exception",
            &format!("Failed to parse query [{text}]: {m}"),
        )
    };
    let toks = lex(text).map_err(fail)?;
    let mut p = Parser { toks, pos: 0, ctx: &ctx };
    let q = p.query(None).map_err(fail)?;
    if p.pos < p.toks.len() {
        return Err(fail("Encountered \")\"".to_string()));
    }
    if let Some(e) = find_error(&q) {
        return Err(EsError::shard_failure("query_shard_exception", &e));
    }
    Ok(match spec.get("boost").and_then(Value::as_f64) {
        Some(b) => json!({"bool": {"must": [q], "boost": b}}),
        None => q,
    })
}

// --- simple_query_string -------------------------------------------------

pub fn simple_query_string(spec: &Value, mappings: &Value) -> Result<Value, EsError> {
    let text = spec
        .get("query")
        .and_then(Value::as_str)
        .ok_or_else(|| EsError::parsing("[simple_query_string] requires 'query' field"))?;
    let fields = parse_fields(spec, &all_fields(mappings), mappings);
    let ctx = Ctx {
        mappings,
        fields,
        and_default: spec
            .get("default_operator")
            .and_then(Value::as_str)
            .is_some_and(|o| o.eq_ignore_ascii_case("and")),
        lenient: true,
        dis_max: false,
        analyze_wildcard: false,
        fuzziness_default: json!("AUTO"),
        phrase_slop: 0,
    };
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    Ok(simple_group(&ctx, &chars, &mut i))
}

/// Lucene's SimpleQueryParser: each new clause joins the running query
/// with the pending operator (explicit `+`/`|`, else the default between
/// whitespace-separated terms); `-` negates the next clause.
fn simple_group(ctx: &Ctx, chars: &[char], i: &mut usize) -> Value {
    let mut top: Option<Value> = None;
    let mut op_and = ctx.and_default;
    let mut explicit_op = false;
    let mut nots = 0;
    while *i < chars.len() {
        let c = chars[*i];
        let branch = match c {
            ' ' | '\t' | '\n' => {
                *i += 1;
                if !explicit_op {
                    op_and = ctx.and_default;
                }
                continue;
            }
            '+' => {
                *i += 1;
                op_and = true;
                explicit_op = true;
                continue;
            }
            '|' => {
                *i += 1;
                op_and = false;
                explicit_op = true;
                continue;
            }
            '-' => {
                *i += 1;
                nots += 1;
                continue;
            }
            '(' => {
                *i += 1;
                let g = simple_group(ctx, chars, i);
                if *i < chars.len() && chars[*i] == ')' {
                    *i += 1;
                }
                Some(g)
            }
            ')' => {
                *i += 1;
                return top.unwrap_or_else(|| json!({"match_none": {}}));
            }
            '"' => {
                *i += 1;
                let start = *i;
                while *i < chars.len() && chars[*i] != '"' {
                    *i += 1;
                }
                let phrase: String = chars[start..*i].iter().collect();
                if *i < chars.len() {
                    *i += 1;
                }
                let slop = simple_tilde(chars, i).unwrap_or(0);
                Some(ctx.phrase_query(None, &phrase, slop))
            }
            _ => {
                let start = *i;
                while *i < chars.len()
                    && !matches!(chars[*i], ' ' | '\t' | '\n' | '+' | '|' | '"' | '(' | ')' | '~')
                {
                    *i += 1;
                }
                let term: String = chars[start..*i].iter().collect();
                let fuzzy = simple_tilde(chars, i).map(|n| Some(n as f64));
                if term.is_empty() {
                    None
                } else if let Some(prefix) = term.strip_suffix('*') {
                    Some(ctx.leaf(None, &|f, ty| {
                        let p = if ty == "keyword" {
                            prefix.to_string()
                        } else {
                            prefix.to_lowercase()
                        };
                        Some(json!({"prefix": {f: p}}))
                    }))
                } else {
                    Some(ctx.term_query(None, &term, fuzzy))
                }
            }
        };
        let Some(mut branch) = branch else { continue };
        if nots % 2 == 1 {
            branch = json!({"bool": {"must_not": [branch], "should": [{"match_all": {}}]}});
        }
        nots = 0;
        top = Some(match top {
            None => branch,
            Some(t) if op_and => json!({"bool": {"must": [t, branch]}}),
            Some(t) => json!({"bool": {"should": [t, branch]}}),
        });
        explicit_op = false;
        op_and = ctx.and_default;
    }
    top.unwrap_or_else(|| json!({"match_none": {}}))
}

fn simple_tilde(chars: &[char], i: &mut usize) -> Option<i64> {
    if *i < chars.len() && chars[*i] == '~' {
        *i += 1;
        let start = *i;
        while *i < chars.len() && chars[*i].is_ascii_digit() {
            *i += 1;
        }
        return Some(chars[start..*i].iter().collect::<String>().parse().unwrap_or(2));
    }
    None
}
