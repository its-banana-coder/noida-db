//! SQL/JSON path (`jsonpath`), as Postgres 14 evaluates it: `lax` (the
//! default: arrays unwrap automatically, structural errors are silent)
//! and `strict` mode; `$`, `@`, `$var`; `.key`, `."key"`, `.*`, `[i]`,
//! `[i to j]`, `[last]`, `[*]`, `.**`; filters `? (...)` with comparisons,
//! `&&`, `||`, `!`, `exists`, `like_regex`, `starts with`, `is unknown`;
//! arithmetic; and the item methods `type`, `size`, `double`, `ceiling`,
//! `floor`, `abs`, `keyvalue`.

use std::cmp::Ordering;

use super::error::{PgError, PgResult};
use crate::sql::json::Json;
use crate::sql::numeric::{self, Numeric};

#[derive(Debug, Clone)]
enum Step {
    Key(String),
    AnyKey,
    AnyIndex,
    Index(Vec<(Expr, Option<Expr>)>),
    Recursive,
    Filter(Expr),
    Method(String),
}

#[derive(Debug, Clone)]
enum Root {
    Dollar,
    At,
    Var(String),
    Lit(Json),
    Last,
    Paren(Box<Expr>),
}

#[derive(Debug, Clone)]
enum Expr {
    Path(Root, Vec<Step>),
    Arith(char, Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    Cmp(&'static str, Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Exists(Box<Expr>),
    Like(Box<Expr>, String, String),
    Starts(Box<Expr>, Box<Expr>),
    IsUnknown(Box<Expr>),
}

pub struct JsonPath {
    strict: bool,
    expr: Expr,
}

fn syntax(path: &str) -> PgError {
    PgError::new(
        "42601",
        format!("syntax error at end of jsonpath input \"{path}\"").replace(" at end", ""),
    )
    .detail(format!("Invalid jsonpath: {path}"))
}

// --- parsing ---------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Dollar,
    At,
    Var(String),
    Ident(String),
    Str(String),
    Num(String),
    Op(&'static str),
}

const OPS: &[&str] = &[
    "==", "!=", "<>", "<=", ">=", "&&", "||", ".**", "<", ">", "!", "(", ")", "[", "]", ".", ",",
    "?", "+", "-", "*", "/", "%", "{", "}",
];

fn lex(s: &str) -> Result<Vec<Tok>, ()> {
    let c: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut out = vec![];
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '$' {
            i += 1;
            let start = i;
            if i < c.len() && c[i] == '"' {
                i += 1;
                let st = i;
                while i < c.len() && c[i] != '"' {
                    i += 1;
                }
                out.push(Tok::Var(c[st..i].iter().collect()));
                i += 1;
                continue;
            }
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_') {
                i += 1;
            }
            if i == start {
                out.push(Tok::Dollar);
            } else {
                out.push(Tok::Var(c[start..i].iter().collect()));
            }
        } else if ch == '@' {
            out.push(Tok::At);
            i += 1;
        } else if ch == '"' {
            i += 1;
            let mut st = String::new();
            while i < c.len() && c[i] != '"' {
                if c[i] == '\\' && i + 1 < c.len() {
                    i += 1;
                    st.push(match c[i] {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        'b' => '\u{8}',
                        'f' => '\u{c}',
                        other => other,
                    });
                } else {
                    st.push(c[i]);
                }
                i += 1;
            }
            if i >= c.len() {
                return Err(());
            }
            i += 1;
            out.push(Tok::Str(st));
        } else if ch.is_ascii_digit()
            || (ch == '.'
                && c.get(i + 1).is_some_and(|n| n.is_ascii_digit())
                && !matches!(
                    out.last(),
                    Some(Tok::Ident(_))
                        | Some(Tok::Op(")"))
                        | Some(Tok::Op("]"))
                        | Some(Tok::Dollar)
                        | Some(Tok::At)
                        | Some(Tok::Var(_))
                ))
        {
            let start = i;
            while i < c.len() && (c[i].is_ascii_digit() || c[i] == '.') {
                i += 1;
            }
            if i < c.len() && (c[i] == 'e' || c[i] == 'E') {
                i += 1;
                if i < c.len() && (c[i] == '+' || c[i] == '-') {
                    i += 1;
                }
                while i < c.len() && c[i].is_ascii_digit() {
                    i += 1;
                }
            }
            out.push(Tok::Num(c[start..i].iter().collect()));
        } else if ch.is_alphabetic() || ch == '_' {
            let start = i;
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_') {
                i += 1;
            }
            out.push(Tok::Ident(c[start..i].iter().collect()));
        } else {
            let rest: String = c[i..c.len().min(i + 3)].iter().collect();
            let op = OPS.iter().find(|o| rest.starts_with(**o)).ok_or(())?;
            out.push(Tok::Op(op));
            i += op.len();
        }
    }
    Ok(out)
}

struct P {
    t: Vec<Tok>,
    i: usize,
    in_filter: usize,
    in_subscript: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn is_op(&self, o: &str) -> bool {
        matches!(self.peek(), Some(Tok::Op(x)) if *x == o)
    }
    fn eat_op(&mut self, o: &str) -> bool {
        if self.is_op(o) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn is_kw(&self, k: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(x)) if x == k)
    }

    fn or(&mut self) -> Result<Expr, ()> {
        let mut l = self.and()?;
        while self.eat_op("||") {
            l = Expr::Or(Box::new(l), Box::new(self.and()?));
        }
        Ok(l)
    }
    fn and(&mut self) -> Result<Expr, ()> {
        let mut l = self.not()?;
        while self.eat_op("&&") {
            l = Expr::And(Box::new(l), Box::new(self.not()?));
        }
        Ok(l)
    }
    fn not(&mut self) -> Result<Expr, ()> {
        if self.eat_op("!") {
            self.expect("(")?;
            let e = self.or()?;
            self.expect(")")?;
            return Ok(Expr::Not(Box::new(e)));
        }
        self.predicate()
    }
    fn expect(&mut self, o: &str) -> Result<(), ()> {
        if self.eat_op(o) { Ok(()) } else { Err(()) }
    }
    fn predicate(&mut self) -> Result<Expr, ()> {
        if self.is_kw("exists") {
            self.i += 1;
            self.expect("(")?;
            let e = self.or()?;
            self.expect(")")?;
            return Ok(Expr::Exists(Box::new(e)));
        }
        let l = self.additive()?;
        for op in ["==", "!=", "<>", "<=", ">=", "<", ">"] {
            if self.eat_op(op) {
                let r = self.additive()?;
                let op: &'static str = match op {
                    "<>" => "!=",
                    o => OPS.iter().find(|x| **x == o).unwrap(),
                };
                return Ok(Expr::Cmp(op, Box::new(l), Box::new(r)));
            }
        }
        if self.is_kw("like_regex") {
            self.i += 1;
            let Some(Tok::Str(pat)) = self.peek().cloned() else { return Err(()) };
            self.i += 1;
            let mut flags = String::new();
            if self.is_kw("flag") {
                self.i += 1;
                let Some(Tok::Str(f)) = self.peek().cloned() else { return Err(()) };
                self.i += 1;
                flags = f;
            }
            return Ok(Expr::Like(Box::new(l), pat, flags));
        }
        if self.is_kw("starts") {
            self.i += 1;
            if !self.is_kw("with") {
                return Err(());
            }
            self.i += 1;
            let r = self.additive()?;
            return Ok(Expr::Starts(Box::new(l), Box::new(r)));
        }
        if self.is_kw("is") {
            self.i += 1;
            if !self.is_kw("unknown") {
                return Err(());
            }
            self.i += 1;
            return Ok(Expr::IsUnknown(Box::new(l)));
        }
        Ok(l)
    }
    fn additive(&mut self) -> Result<Expr, ()> {
        let mut l = self.multiplicative()?;
        loop {
            let op = if self.eat_op("+") {
                '+'
            } else if self.eat_op("-") {
                '-'
            } else {
                break;
            };
            l = Expr::Arith(op, Box::new(l), Box::new(self.multiplicative()?));
        }
        Ok(l)
    }
    fn multiplicative(&mut self) -> Result<Expr, ()> {
        let mut l = self.unary()?;
        loop {
            let op = if self.eat_op("*") {
                '*'
            } else if self.eat_op("/") {
                '/'
            } else if self.eat_op("%") {
                '%'
            } else {
                break;
            };
            l = Expr::Arith(op, Box::new(l), Box::new(self.unary()?));
        }
        Ok(l)
    }
    fn unary(&mut self) -> Result<Expr, ()> {
        if self.eat_op("-") {
            return Ok(Expr::Neg(Box::new(self.unary()?)));
        }
        if self.eat_op("+") {
            return self.unary();
        }
        self.accessor()
    }
    fn accessor(&mut self) -> Result<Expr, ()> {
        let root = match self.peek().cloned() {
            Some(Tok::Dollar) => {
                self.i += 1;
                Root::Dollar
            }
            Some(Tok::At) if self.in_filter > 0 => {
                self.i += 1;
                Root::At
            }
            Some(Tok::Var(v)) => {
                self.i += 1;
                Root::Var(v)
            }
            Some(Tok::Num(n)) => {
                self.i += 1;
                Root::Lit(Json::Num(Numeric::parse(&n).map_err(|_| ())?))
            }
            Some(Tok::Str(s)) => {
                self.i += 1;
                Root::Lit(Json::Str(s))
            }
            Some(Tok::Ident(k)) if k == "true" => {
                self.i += 1;
                Root::Lit(Json::Bool(true))
            }
            Some(Tok::Ident(k)) if k == "false" => {
                self.i += 1;
                Root::Lit(Json::Bool(false))
            }
            Some(Tok::Ident(k)) if k == "null" => {
                self.i += 1;
                Root::Lit(Json::Null)
            }
            Some(Tok::Ident(k)) if k == "last" && self.in_subscript > 0 => {
                self.i += 1;
                Root::Last
            }
            Some(Tok::Op("(")) => {
                self.i += 1;
                let e = self.or()?;
                self.expect(")")?;
                Root::Paren(Box::new(e))
            }
            _ => return Err(()),
        };
        let mut steps = vec![];
        loop {
            if self.eat_op(".**") {
                // `.**{2 to last}` level ranges aren't modelled.
                if self.is_op("{") {
                    return Err(());
                }
                steps.push(Step::Recursive);
            } else if self.eat_op(".") {
                match self.peek().cloned() {
                    Some(Tok::Op("*")) => {
                        self.i += 1;
                        steps.push(Step::AnyKey);
                    }
                    Some(Tok::Str(s)) => {
                        self.i += 1;
                        steps.push(Step::Key(s));
                    }
                    Some(Tok::Ident(k)) => {
                        self.i += 1;
                        if self.eat_op("(") {
                            self.expect(")")?;
                            steps.push(Step::Method(k));
                        } else {
                            steps.push(Step::Key(k));
                        }
                    }
                    Some(Tok::Var(v)) => {
                        self.i += 1;
                        steps.push(Step::Key(format!("${v}")));
                    }
                    _ => return Err(()),
                }
            } else if self.eat_op("[") {
                if self.eat_op("*") {
                    self.expect("]")?;
                    steps.push(Step::AnyIndex);
                    continue;
                }
                let mut subs = vec![];
                self.in_subscript += 1;
                loop {
                    let from = self.additive()?;
                    let to = if self.is_kw("to") {
                        self.i += 1;
                        Some(self.additive()?)
                    } else {
                        None
                    };
                    subs.push((from, to));
                    if !self.eat_op(",") {
                        break;
                    }
                }
                self.in_subscript -= 1;
                self.expect("]")?;
                steps.push(Step::Index(subs));
            } else if self.eat_op("?") {
                self.expect("(")?;
                self.in_filter += 1;
                let e = self.or()?;
                self.in_filter -= 1;
                self.expect(")")?;
                steps.push(Step::Filter(e));
            } else {
                break;
            }
        }
        Ok(Expr::Path(root, steps))
    }
}

pub fn parse(text: &str) -> PgResult<JsonPath> {
    let toks = lex(text).map_err(|_| syntax(text))?;
    let mut p = P { t: toks, i: 0, in_filter: 0, in_subscript: 0 };
    let mut strict = false;
    if p.is_kw("strict") {
        strict = true;
        p.i += 1;
    } else if p.is_kw("lax") {
        p.i += 1;
    }
    let expr = p.or().map_err(|_| syntax(text))?;
    if p.i < p.t.len() {
        return Err(syntax(text));
    }
    Ok(JsonPath { strict, expr })
}

// --- evaluation --------------------------------------------------------------

struct Ev<'a> {
    strict: bool,
    root: &'a Json,
    vars: &'a Json,
}

type R<T> = Result<T, PgError>;

fn err(code: &'static str, msg: &str) -> PgError {
    PgError::new(code, msg.to_string())
}

fn num(v: &Json) -> Option<&Numeric> {
    match v {
        Json::Num(n) => Some(n),
        _ => None,
    }
}

impl Ev<'_> {
    fn eval(&self, e: &Expr, at: &Json, last: Option<i64>) -> R<Vec<Json>> {
        match e {
            Expr::Path(root, steps) => {
                let start: Vec<Json> = match root {
                    Root::Dollar => vec![self.root.clone()],
                    Root::At => vec![at.clone()],
                    Root::Lit(j) => vec![j.clone()],
                    Root::Last => match last {
                        Some(l) => vec![Json::Num(Numeric::from_i64(l))],
                        None => {
                            return Err(err("42601", "LAST is allowed only in array subscripts"));
                        }
                    },
                    Root::Var(v) => match self.vars {
                        Json::Object(m) => match m.iter().find(|(k, _)| k == v) {
                            Some((_, val)) => vec![val.clone()],
                            None => {
                                return Err(err(
                                    "42704",
                                    &format!("could not find jsonpath variable \"{v}\""),
                                ));
                            }
                        },
                        _ => {
                            return Err(err(
                                "42704",
                                &format!("could not find jsonpath variable \"{v}\""),
                            ));
                        }
                    },
                    Root::Paren(inner) => self.eval(inner, at, last)?,
                };
                let mut cur = start;
                for st in steps {
                    let mut next = vec![];
                    for item in cur {
                        self.step(st, &item, &mut next)?;
                    }
                    cur = next;
                }
                Ok(cur)
            }
            Expr::Arith(op, l, r) => {
                let (a, b) = (self.singleton_num(l, at, last)?, self.singleton_num(r, at, last)?);
                let v = match op {
                    '+' => a.add(&b),
                    '-' => a.sub(&b),
                    '*' => a.mul(&b),
                    '/' => a.div(&b).map_err(|_| err("22012", "division by zero"))?,
                    _ => a.rem(&b).map_err(|_| err("22012", "division by zero"))?,
                };
                Ok(vec![Json::Num(v)])
            }
            Expr::Neg(x) => {
                let mut out = vec![];
                for v in self.unwrap(self.eval(x, at, last)?) {
                    match v {
                        Json::Num(n) => out.push(Json::Num(n.neg())),
                        _ => {
                            return Err(err(
                                "22038",
                                "operand of unary jsonpath operator - is not a numeric value",
                            ));
                        }
                    }
                }
                Ok(out)
            }
            // A predicate used as a value: true / false / null (unknown).
            other => Ok(vec![match self.pred(other, at, last)? {
                Some(b) => Json::Bool(b),
                None => Json::Null,
            }]),
        }
    }

    fn unwrap(&self, vs: Vec<Json>) -> Vec<Json> {
        if self.strict {
            return vs;
        }
        vs.into_iter()
            .flat_map(|v| match v {
                Json::Array(a) => a,
                other => vec![other],
            })
            .collect()
    }

    fn singleton_num(&self, e: &Expr, at: &Json, last: Option<i64>) -> R<Numeric> {
        let vs = self.unwrap(self.eval(e, at, last)?);
        match vs.as_slice() {
            [Json::Num(n)] => Ok(n.clone()),
            _ => Err(err(
                "22038",
                "right operand of jsonpath operator is not a single numeric value",
            )),
        }
    }

    fn step(&self, st: &Step, item: &Json, out: &mut Vec<Json>) -> R<()> {
        match st {
            Step::Key(k) => match item {
                Json::Object(m) => match m.iter().find(|(x, _)| x == k) {
                    Some((_, v)) => out.push(v.clone()),
                    None if self.strict => {
                        return Err(err(
                            "2203A",
                            &format!("JSON object does not contain key \"{k}\""),
                        ));
                    }
                    None => {}
                },
                Json::Array(a) if !self.strict => {
                    for e in a {
                        self.step(st, e, out)?;
                    }
                }
                _ if self.strict => {
                    return Err(err(
                        "2203A",
                        "jsonpath member accessor can only be applied to an object",
                    ));
                }
                _ => {}
            },
            Step::AnyKey => match item {
                Json::Object(m) => out.extend(m.iter().map(|(_, v)| v.clone())),
                Json::Array(a) if !self.strict => {
                    for e in a {
                        self.step(st, e, out)?;
                    }
                }
                _ if self.strict => {
                    return Err(err(
                        "2203C",
                        "jsonpath wildcard member accessor can only be applied to an object",
                    ));
                }
                _ => {}
            },
            Step::AnyIndex => match item {
                Json::Array(a) => out.extend(a.iter().cloned()),
                _ if self.strict => {
                    return Err(err(
                        "22039",
                        "jsonpath wildcard array accessor can only be applied to an array",
                    ));
                }
                other => out.push(other.clone()),
            },
            Step::Index(subs) => {
                let arr: Vec<Json> = match item {
                    Json::Array(a) => a.clone(),
                    _ if self.strict => {
                        return Err(err(
                            "22039",
                            "jsonpath array accessor can only be applied to an array",
                        ));
                    }
                    other => vec![other.clone()],
                };
                let last = arr.len() as i64 - 1;
                for (from, to) in subs {
                    let idx = |e: &Expr| -> R<i64> {
                        let vs = self.unwrap(self.eval(e, item, Some(last))?);
                        match vs.as_slice() {
                            [Json::Num(n)] => Ok(n.trunc(0).to_i64().unwrap_or(i64::MAX)),
                            _ => Err(err(
                                "22033",
                                "jsonpath array subscript is not a single numeric value",
                            )),
                        }
                    };
                    let a = idx(from)?;
                    let b = match to {
                        Some(t) => idx(t)?,
                        None => a,
                    };
                    if self.strict && (a < 0 || b > last || a > b) {
                        return Err(err("22033", "jsonpath array subscript is out of bounds"));
                    }
                    let (a, b) = (a.max(0), b.min(last));
                    if a <= b {
                        out.extend(arr[a as usize..=b as usize].iter().cloned());
                    }
                }
            }
            Step::Recursive => {
                fn all(v: &Json, out: &mut Vec<Json>) {
                    out.push(v.clone());
                    match v {
                        Json::Array(a) => a.iter().for_each(|e| all(e, out)),
                        Json::Object(m) => m.iter().for_each(|(_, e)| all(e, out)),
                        _ => {}
                    }
                }
                all(item, out);
            }
            Step::Filter(pred) => {
                let items: Vec<Json> = match item {
                    Json::Array(a) if !self.strict => a.clone(),
                    other => vec![other.clone()],
                };
                for it in items {
                    if self.pred(pred, &it, None)? == Some(true) {
                        out.push(it);
                    }
                }
            }
            Step::Method(m) => self.method(m, item, out)?,
        }
        Ok(())
    }

    fn method(&self, m: &str, item: &Json, out: &mut Vec<Json>) -> R<()> {
        // Lax mode applies methods (other than type/size) to array elements.
        if !self.strict
            && let Json::Array(a) = item
            && !matches!(m, "type" | "size")
        {
            for e in a {
                self.method(m, e, out)?;
            }
            return Ok(());
        }
        match m {
            "type" => out.push(Json::Str(
                match item {
                    Json::Null => "null",
                    Json::Bool(_) => "boolean",
                    Json::Num(_) => "number",
                    Json::Str(_) => "string",
                    Json::Array(_) => "array",
                    Json::Object(_) => "object",
                }
                .to_string(),
            )),
            "size" => match item {
                Json::Array(a) => out.push(Json::Num(Numeric::from_i64(a.len() as i64))),
                _ if self.strict => {
                    return Err(err(
                        "22039",
                        "jsonpath item method .size() can only be applied to an array",
                    ));
                }
                _ => out.push(Json::Num(Numeric::from_i64(1))),
            },
            "double" => match item {
                Json::Num(n) => out.push(Json::Num(Numeric::from_f64(n.to_f64()))),
                Json::Str(s) => match s.trim().parse::<f64>() {
                    Ok(f) if f.is_finite() => out.push(Json::Num(Numeric::from_f64(f))),
                    _ => {
                        return Err(err(
                            "22038",
                            "string argument of jsonpath item method .double() is not a valid representation of a double precision number",
                        ));
                    }
                },
                _ => {
                    return Err(err(
                        "22038",
                        "jsonpath item method .double() can only be applied to a string or numeric value",
                    ));
                }
            },
            "ceiling" | "floor" | "abs" => match num(item) {
                Some(n) => out.push(Json::Num(match m {
                    "ceiling" => n.ceil(),
                    "floor" => n.floor(),
                    _ => n.abs(),
                })),
                None => {
                    return Err(err(
                        "22038",
                        &format!(
                            "jsonpath item method .{m}() can only be applied to a numeric value"
                        ),
                    ));
                }
            },
            "keyvalue" => match item {
                Json::Object(fields) => {
                    for (k, v) in fields {
                        out.push(Json::Object(vec![
                            ("id".into(), Json::Num(Numeric::zero())),
                            ("key".into(), Json::Str(k.clone())),
                            ("value".into(), v.clone()),
                        ]));
                    }
                }
                _ => {
                    return Err(err(
                        "2203C",
                        "jsonpath item method .keyvalue() can only be applied to an object",
                    ));
                }
            },
            other => {
                return Err(err(
                    "42601",
                    &format!("syntax error at or near \"{other}\" of jsonpath input"),
                ));
            }
        }
        Ok(())
    }

    /// Three-valued: `None` is unknown (an error inside a predicate). A
    /// missing variable is still an error.
    fn pred(&self, e: &Expr, at: &Json, last: Option<i64>) -> R<Option<bool>> {
        self.pred_inner(e, at, last)
    }

    fn eval_soft(&self, e: &Expr, at: &Json, last: Option<i64>) -> R<Option<Vec<Json>>> {
        match self.eval(e, at, last) {
            Ok(v) => Ok(Some(v)),
            Err(err) if err.code == "42704" => Err(err),
            Err(_) => Ok(None),
        }
    }

    fn pred_inner(&self, e: &Expr, at: &Json, last: Option<i64>) -> R<Option<bool>> {
        Ok(match e {
            Expr::And(a, b) => match (self.pred(a, at, last)?, self.pred(b, at, last)?) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Expr::Or(a, b) => match (self.pred(a, at, last)?, self.pred(b, at, last)?) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            Expr::Not(a) => self.pred(a, at, last)?.map(|b| !b),
            Expr::IsUnknown(a) => Some(self.pred(a, at, last)?.is_none()),
            Expr::Exists(p) => self.eval_soft(p, at, last)?.map(|v| !v.is_empty()),
            Expr::Cmp(op, l, r) => {
                let (Some(ls), Some(rs)) =
                    (self.eval_soft(l, at, last)?, self.eval_soft(r, at, last)?)
                else {
                    return Ok(None);
                };
                let (ls, rs) = (self.unwrap(ls), self.unwrap(rs));
                let mut any_true = false;
                let mut any_err = false;
                for a in &ls {
                    for b in &rs {
                        // null against a non-null: only `!=` holds.
                        if matches!(a, Json::Null) != matches!(b, Json::Null) {
                            any_true |= *op == "!=";
                            continue;
                        }
                        match compare(a, b) {
                            None => any_err = true,
                            Some(o) => {
                                let t = match *op {
                                    "==" => o == Ordering::Equal,
                                    "!=" => o != Ordering::Equal,
                                    "<" => o == Ordering::Less,
                                    "<=" => o != Ordering::Greater,
                                    ">" => o == Ordering::Greater,
                                    _ => o != Ordering::Less,
                                };
                                any_true |= t;
                            }
                        }
                    }
                }
                if any_true {
                    Some(true)
                } else if any_err {
                    None
                } else {
                    Some(false)
                }
            }
            Expr::Like(x, pat, flags) => {
                let Some(vs) = self.eval_soft(x, at, last)? else { return Ok(None) };
                let mut prefix = String::new();
                for f in flags.chars() {
                    match f {
                        'i' => prefix.push_str("(?i)"),
                        's' => prefix.push_str("(?s)"),
                        'm' => prefix.push_str("(?m)"),
                        'x' => prefix.push_str("(?x)"),
                        'q' => {}
                        _ => return Err(err("42601", "invalid input syntax for type jsonpath")),
                    }
                }
                let body = if flags.contains('q') { regex_lite::escape(pat) } else { pat.clone() };
                let re = regex_lite::Regex::new(&(prefix + &body))
                    .map_err(|_| err("2201B", "invalid regular expression"))?;
                let mut res = Some(false);
                for v in self.unwrap(vs) {
                    match v {
                        Json::Str(s) => {
                            if re.is_match(&s) {
                                return Ok(Some(true));
                            }
                        }
                        _ => res = None,
                    }
                }
                res
            }
            Expr::Starts(x, p) => {
                let (Some(vs), Some(ps)) =
                    (self.eval_soft(x, at, last)?, self.eval_soft(p, at, last)?)
                else {
                    return Ok(None);
                };
                let Some(Json::Str(prefix)) = ps.into_iter().next() else { return Ok(None) };
                let mut res = Some(false);
                for v in self.unwrap(vs) {
                    match v {
                        Json::Str(s) if s.starts_with(&prefix) => return Ok(Some(true)),
                        Json::Str(_) => {}
                        _ => res = None,
                    }
                }
                res
            }
            // A plain path used as a predicate (`@@ '$.a'`): its value must
            // be a boolean.
            other => match self.eval(other, at, last)?.as_slice() {
                [Json::Bool(b)] => Some(*b),
                [Json::Null] => None,
                _ => None,
            },
        })
    }
}

/// SQL/JSON comparison: same-kind scalars compare; anything else (and
/// arrays/objects) is an error, i.e. unknown. null == null is true.
fn compare(a: &Json, b: &Json) -> Option<Ordering> {
    match (a, b) {
        (Json::Null, Json::Null) => Some(Ordering::Equal),
        (Json::Num(x), Json::Num(y)) => Some(numeric::cmp_num(x, y)),
        (Json::Str(x), Json::Str(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
        (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

impl JsonPath {
    /// The items the path yields. With `silent`, structural and type errors
    /// give an empty result instead.
    pub fn query(&self, doc: &Json, vars: &Json, silent: bool) -> PgResult<Vec<Json>> {
        let ev = Ev { strict: self.strict, root: doc, vars };
        match ev.eval(&self.expr, doc, None) {
            Ok(v) => Ok(v),
            Err(e) if silent && e.code != "42704" => Ok(vec![]),
            Err(e) => Err(e),
        }
    }

    /// `@@` / `jsonb_path_match`: the single boolean the path yields.
    pub fn matches(&self, doc: &Json, vars: &Json, silent: bool) -> PgResult<Option<bool>> {
        let items = self.query(doc, vars, silent)?;
        match items.as_slice() {
            [Json::Bool(b)] => Ok(Some(*b)),
            [Json::Null] => Ok(None),
            _ if silent => Ok(None),
            _ => Err(err("22038", "single boolean result is expected")),
        }
    }
}
