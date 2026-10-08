//! A small Painless interpreter for update scripts (`_update`,
//! `_update_by_query`): the subset those scripts use in practice —
//! `ctx._source` reads and writes (`=`, `+=`, `++`, ...), `params`, local
//! `def` variables, `if`/`else`, `for`/`for-each`/`while`, `return`, the
//! usual operators (Java integer arithmetic included), string, list and
//! map methods, `new ArrayList()`/`new HashMap()`, `Math.*`, and
//! `ctx.op = 'noop' | 'delete'`. Values are JSON; a script error is
//! reported the way Elasticsearch reports a failed script.

use serde_json::{Map, Number, Value, json};

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(Value),
    Str(String),
    Ident(String),
    Op(&'static str),
}

const OPS: &[&str] = &[
    "===", "!==", ">>=", "<<=", "==", "!=", "<=", ">=", "&&", "||", "++", "--", "+=", "-=", "*=",
    "/=", "%=", "?.", "->", "+", "-", "*", "/", "%", "=", "<", ">", "!", "(", ")", "{", "}", "[",
    "]", ".", ",", ";", "?", ":",
];

fn lex(src: &str) -> Result<Vec<Tok>, String> {
    let c: Vec<char> = src.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '/' && c.get(i + 1) == Some(&'/') {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch == '/' && c.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < c.len() && !(c[i] == '*' && c[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if ch.is_ascii_digit() {
            let start = i;
            while i < c.len() && (c[i].is_ascii_digit() || c[i] == '.') {
                i += 1;
            }
            let s: String = c[start..i].iter().collect();
            // Java suffixes: 1L, 2.0f, 3d.
            if i < c.len() && "lLfFdD".contains(c[i]) {
                i += 1;
            }
            out.push(Tok::Num(if s.contains('.') {
                json!(s.parse::<f64>().map_err(|_| format!("bad number {s}"))?)
            } else {
                json!(s.parse::<i64>().map_err(|_| format!("bad number {s}"))?)
            }));
        } else if ch == '\'' || ch == '"' {
            i += 1;
            let mut s = String::new();
            while i < c.len() && c[i] != ch {
                if c[i] == '\\' && i + 1 < c.len() {
                    i += 1;
                    s.push(match c[i] {
                        'n' => '\n',
                        't' => '\t',
                        other => other,
                    });
                } else {
                    s.push(c[i]);
                }
                i += 1;
            }
            if i >= c.len() {
                return Err("unterminated string".into());
            }
            i += 1;
            out.push(Tok::Str(s));
        } else if ch.is_alphabetic() || ch == '_' || ch == '$' {
            let start = i;
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '$') {
                i += 1;
            }
            out.push(Tok::Ident(c[start..i].iter().collect()));
        } else {
            let rest: String = c[i..c.len().min(i + 3)].iter().collect();
            let Some(op) = OPS.iter().find(|o| rest.starts_with(**o)) else {
                return Err(format!("unexpected character [{ch}]"));
            };
            out.push(Tok::Op(op));
            i += op.len();
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
enum Expr {
    Lit(Value),
    Var(String),
    Field(Box<Expr>, String, bool),
    Index(Box<Expr>, Box<Expr>),
    Call(Box<Expr>, String, Vec<Expr>),
    Static(String, String, Vec<Expr>),
    New(String, Vec<Expr>),
    List(Vec<Expr>),
    MapLit(Vec<(Expr, Expr)>),
    Unary(&'static str, Box<Expr>),
    Binary(&'static str, Box<Expr>, Box<Expr>),
    Assign(&'static str, Box<Expr>, Box<Expr>),
    IncDec(&'static str, bool, Box<Expr>),
    Ternary(Box<Expr>, Box<Expr>, Box<Expr>),
    Lambda(Vec<String>, Box<Expr>),
    Cast(Box<Expr>),
}

#[derive(Debug, Clone)]
enum Stmt {
    Expr(Expr),
    Decl(String, Option<Expr>),
    If(Expr, Vec<Stmt>, Vec<Stmt>),
    While(Expr, Vec<Stmt>),
    For(Option<Box<Stmt>>, Option<Expr>, Option<Expr>, Vec<Stmt>),
    ForEach(String, Expr, Vec<Stmt>),
    Return(Option<Expr>),
    Break,
    Continue,
}

const TYPES: &[&str] = &[
    "def",
    "int",
    "long",
    "double",
    "float",
    "boolean",
    "String",
    "List",
    "Map",
    "Object",
    "short",
    "byte",
    "char",
    "ArrayList",
    "HashMap",
    "var",
];

struct P {
    t: Vec<Tok>,
    i: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn is(&self, op: &str) -> bool {
        matches!(self.peek(), Some(Tok::Op(o)) if *o == op)
    }
    fn eat(&mut self, op: &str) -> bool {
        if self.is(op) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, op: &str) -> Result<(), String> {
        if self.eat(op) {
            Ok(())
        } else {
            Err(format!("expected [{op}] but found [{:?}]", self.peek()))
        }
    }
    fn ident(&mut self) -> Result<String, String> {
        match self.t.get(self.i).cloned() {
            Some(Tok::Ident(s)) => {
                self.i += 1;
                Ok(s)
            }
            other => Err(format!("expected a name, found [{other:?}]")),
        }
    }
    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(s)) if s == kw)
    }

    fn block(&mut self) -> Result<Vec<Stmt>, String> {
        if self.eat("{") {
            let mut out = Vec::new();
            while !self.eat("}") {
                if self.peek().is_none() {
                    return Err("unexpected end of script".into());
                }
                out.push(self.stmt()?);
            }
            Ok(out)
        } else {
            Ok(vec![self.stmt()?])
        }
    }

    fn stmt(&mut self) -> Result<Stmt, String> {
        while self.eat(";") {}
        if self.is_kw("if") {
            self.i += 1;
            self.expect("(")?;
            let c = self.expr()?;
            self.expect(")")?;
            let then = self.block()?;
            let els = if self.is_kw("else") {
                self.i += 1;
                self.block()?
            } else {
                Vec::new()
            };
            return Ok(Stmt::If(c, then, els));
        }
        if self.is_kw("while") {
            self.i += 1;
            self.expect("(")?;
            let c = self.expr()?;
            self.expect(")")?;
            return Ok(Stmt::While(c, self.block()?));
        }
        if self.is_kw("for") {
            self.i += 1;
            self.expect("(")?;
            // for (def x : list) / for (x in list)
            let save = self.i;
            if let Some(Tok::Ident(t)) = self.peek().cloned()
                && TYPES.contains(&t.as_str())
            {
                self.i += 1;
            }
            if let Ok(name) = self.ident()
                && (self.eat(":")
                    || (self.is_kw("in") && {
                        self.i += 1;
                        true
                    }))
            {
                let iter = self.expr()?;
                self.expect(")")?;
                return Ok(Stmt::ForEach(name, iter, self.block()?));
            }
            self.i = save;
            let init = if self.is(";") { None } else { Some(Box::new(self.simple_stmt()?)) };
            self.expect(";")?;
            let cond = if self.is(";") { None } else { Some(self.expr()?) };
            self.expect(";")?;
            let step = if self.is(")") { None } else { Some(self.expr()?) };
            self.expect(")")?;
            return Ok(Stmt::For(init, cond, step, self.block()?));
        }
        let s = self.simple_stmt()?;
        if !self.eat(";") && !self.is("}") && self.peek().is_some() {
            return Err(format!("unexpected token [{:?}]", self.peek()));
        }
        Ok(s)
    }

    fn simple_stmt(&mut self) -> Result<Stmt, String> {
        if self.is_kw("return") {
            self.i += 1;
            if self.is(";") || self.is("}") || self.peek().is_none() {
                return Ok(Stmt::Return(None));
            }
            return Ok(Stmt::Return(Some(self.expr()?)));
        }
        if self.is_kw("break") {
            self.i += 1;
            return Ok(Stmt::Break);
        }
        if self.is_kw("continue") {
            self.i += 1;
            return Ok(Stmt::Continue);
        }
        // `def x = ...`, `int x = ...`, `List l = ...` (a type then a name).
        if let (Some(Tok::Ident(t)), Some(Tok::Ident(_))) =
            (self.t.get(self.i), self.t.get(self.i + 1))
            && (TYPES.contains(&t.as_str()) || t.chars().next().is_some_and(char::is_uppercase))
        {
            self.i += 1;
            let name = self.ident()?;
            let init = if self.eat("=") { Some(self.expr()?) } else { None };
            return Ok(Stmt::Decl(name, init));
        }
        Ok(Stmt::Expr(self.expr()?))
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let lhs = self.ternary()?;
        for op in ["=", "+=", "-=", "*=", "/=", "%="] {
            if self.eat(op) {
                let rhs = self.expr()?;
                let op: &'static str = OPS.iter().find(|o| **o == op).unwrap();
                return Ok(Expr::Assign(op, Box::new(lhs), Box::new(rhs)));
            }
        }
        Ok(lhs)
    }

    fn ternary(&mut self) -> Result<Expr, String> {
        let c = self.binary(0)?;
        if self.eat("?") {
            let a = self.expr()?;
            self.expect(":")?;
            let b = self.expr()?;
            return Ok(Expr::Ternary(Box::new(c), Box::new(a), Box::new(b)));
        }
        Ok(c)
    }

    fn binary(&mut self, level: usize) -> Result<Expr, String> {
        const LEVELS: &[&[&str]] = &[
            &["||"],
            &["&&"],
            &["==", "!=", "===", "!=="],
            &["<", ">", "<=", ">="],
            &["+", "-"],
            &["*", "/", "%"],
        ];
        if level == LEVELS.len() {
            return self.unary();
        }
        let mut lhs = self.binary(level + 1)?;
        loop {
            if level == 3 && self.is_kw("instanceof") {
                self.i += 1;
                let ty = self.ident()?;
                lhs = Expr::Static("instanceof".into(), ty, vec![lhs]);
                continue;
            }
            let Some(op) = LEVELS[level].iter().find(|o| self.is(o)) else { break };
            self.i += 1;
            let op: &'static str = OPS.iter().find(|o| *o == op).unwrap();
            let rhs = self.binary(level + 1)?;
            lhs = Expr::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn unary(&mut self) -> Result<Expr, String> {
        for op in ["!", "-", "+", "++", "--"] {
            if self.is(op) {
                self.i += 1;
                let e = self.unary()?;
                let op: &'static str = OPS.iter().find(|o| **o == op).unwrap();
                return Ok(match op {
                    "++" | "--" => Expr::IncDec(op, true, Box::new(e)),
                    _ => Expr::Unary(op, Box::new(e)),
                });
            }
        }
        // `(int) x` casts.
        if self.is("(")
            && let (Some(Tok::Ident(t)), Some(Tok::Op(")"))) =
                (self.t.get(self.i + 1), self.t.get(self.i + 2))
            && TYPES.contains(&t.as_str())
        {
            let ty = t.clone();
            self.i += 3;
            let e = self.unary()?;
            return Ok(match ty.as_str() {
                "int" | "long" | "short" | "byte" => {
                    Expr::Static("cast".into(), "long".into(), vec![e])
                }
                "double" | "float" => Expr::Static("cast".into(), "double".into(), vec![e]),
                _ => Expr::Cast(Box::new(e)),
            });
        }
        let mut e = self.postfix_base()?;
        loop {
            if self.eat(".") || self.is("?.") {
                let safe = if self.is("?.") {
                    self.i += 1;
                    true
                } else {
                    false
                };
                let name = self.ident()?;
                if self.eat("(") {
                    let args = self.args(")")?;
                    e = Expr::Call(Box::new(e), name, args);
                } else {
                    e = Expr::Field(Box::new(e), name, safe);
                }
            } else if self.eat("[") {
                let idx = self.expr()?;
                self.expect("]")?;
                e = Expr::Index(Box::new(e), Box::new(idx));
            } else if self.is("++") || self.is("--") {
                let op = if self.eat("++") {
                    "++"
                } else {
                    self.i += 1;
                    "--"
                };
                e = Expr::IncDec(op, false, Box::new(e));
            } else {
                break;
            }
        }
        Ok(e)
    }

    fn args(&mut self, close: &str) -> Result<Vec<Expr>, String> {
        let mut out = Vec::new();
        if self.eat(close) {
            return Ok(out);
        }
        loop {
            out.push(self.lambda_or_expr()?);
            if self.eat(close) {
                return Ok(out);
            }
            self.expect(",")?;
        }
    }

    fn lambda_or_expr(&mut self) -> Result<Expr, String> {
        // `x -> expr` or `(a, b) -> expr`
        if let (Some(Tok::Ident(n)), Some(Tok::Op("->"))) =
            (self.t.get(self.i), self.t.get(self.i + 1))
        {
            let n = n.clone();
            self.i += 2;
            return Ok(Expr::Lambda(vec![n], Box::new(self.expr()?)));
        }
        if self.is("(") {
            let save = self.i;
            self.i += 1;
            let mut names = Vec::new();
            while let Some(Tok::Ident(n)) = self.peek().cloned() {
                self.i += 1;
                names.push(n);
                if !self.eat(",") {
                    break;
                }
            }
            if self.eat(")") && self.eat("->") {
                return Ok(Expr::Lambda(names, Box::new(self.expr()?)));
            }
            self.i = save;
        }
        self.expr()
    }

    fn postfix_base(&mut self) -> Result<Expr, String> {
        match self.t.get(self.i).cloned() {
            Some(Tok::Num(n)) => {
                self.i += 1;
                Ok(Expr::Lit(n))
            }
            Some(Tok::Str(s)) => {
                self.i += 1;
                Ok(Expr::Lit(json!(s)))
            }
            Some(Tok::Op("(")) => {
                self.i += 1;
                let e = self.expr()?;
                self.expect(")")?;
                Ok(e)
            }
            Some(Tok::Op("[")) => {
                self.i += 1;
                if self.eat(":") {
                    self.expect("]")?;
                    return Ok(Expr::MapLit(Vec::new()));
                }
                let first = self.args("]")?;
                Ok(Expr::List(first))
            }
            Some(Tok::Ident(id)) => {
                self.i += 1;
                match id.as_str() {
                    "true" => Ok(Expr::Lit(json!(true))),
                    "false" => Ok(Expr::Lit(json!(false))),
                    "null" => Ok(Expr::Lit(Value::Null)),
                    "new" => {
                        let ty = self.ident()?;
                        // generics: new ArrayList<>()
                        if self.eat("<") {
                            while !self.eat(">") {
                                self.i += 1;
                            }
                        }
                        self.expect("(")?;
                        let args = self.args(")")?;
                        Ok(Expr::New(ty, args))
                    }
                    _ if id.chars().next().is_some_and(char::is_uppercase) && self.is(".") => {
                        self.i += 1;
                        let m = self.ident()?;
                        let args = if self.eat("(") { self.args(")")? } else { Vec::new() };
                        Ok(Expr::Static(id, m, args))
                    }
                    _ => Ok(Expr::Var(id)),
                }
            }
            other => Err(format!("unexpected token [{other:?}]")),
        }
    }
}

pub struct Script {
    stmts: Vec<Stmt>,
}

pub fn compile(src: &str) -> Result<Script, String> {
    let toks = lex(src)?;
    let mut p = P { t: toks, i: 0 };
    let mut stmts = Vec::new();
    while p.peek().is_some() {
        if p.eat(";") {
            continue;
        }
        stmts.push(p.stmt()?);
    }
    Ok(Script { stmts })
}

enum Flow {
    Normal,
    Break,
    Continue,
    Return(Value),
}

struct Env {
    scopes: Vec<Map<String, Value>>,
    steps: u64,
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
}

fn is_int(v: &Value) -> bool {
    v.as_i64().is_some() || v.as_u64().is_some()
}

fn truthy(v: &Value) -> Result<bool, String> {
    match v {
        Value::Bool(b) => Ok(*b),
        other => Err(format!("Cannot cast {} to boolean", type_name(other))),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "java.lang.Boolean",
        Value::Number(n) if n.is_i64() => "java.lang.Integer",
        Value::Number(_) => "java.lang.Double",
        Value::String(_) => "java.lang.String",
        Value::Array(_) => "java.util.ArrayList",
        Value::Object(_) => "java.util.HashMap",
    }
}

fn to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().unwrap();
            if f.fract() == 0.0 && f.abs() < 1e7 { format!("{f:.1}") } else { f.to_string() }
        }
        Value::Array(a) => format!("[{}]", a.iter().map(to_string).collect::<Vec<_>>().join(", ")),
        Value::Object(m) => format!(
            "{{{}}}",
            m.iter().map(|(k, v)| format!("{k}={}", to_string(v))).collect::<Vec<_>>().join(", ")
        ),
        other => other.to_string(),
    }
}

fn arith(op: &str, a: &Value, b: &Value) -> Result<Value, String> {
    if op == "+" && (a.is_string() || b.is_string()) {
        return Ok(json!(format!("{}{}", to_string(a), to_string(b))));
    }
    let (Some(x), Some(y)) = (num(a), num(b)) else {
        if a.is_null() || b.is_null() {
            return Err(
                "Cannot invoke \"java.lang.Number.intValue()\" because value is null".into()
            );
        }
        return Err(format!(
            "Cannot apply [{op}] operation to types [{}] and [{}].",
            type_name(a),
            type_name(b)
        ));
    };
    if is_int(a) && is_int(b) {
        let (x, y) = (a.as_i64().unwrap_or(0), b.as_i64().unwrap_or(0));
        return Ok(json!(match op {
            "+" => x.wrapping_add(y),
            "-" => x.wrapping_sub(y),
            "*" => x.wrapping_mul(y),
            "/" => {
                if y == 0 {
                    return Err("/ by zero".into());
                }
                x / y
            }
            _ => {
                if y == 0 {
                    return Err("/ by zero".into());
                }
                x % y
            }
        }));
    }
    let r = match op {
        "+" => x + y,
        "-" => x - y,
        "*" => x * y,
        "/" => x / y,
        _ => x % y,
    };
    Ok(Number::from_f64(r).map(Value::Number).unwrap_or(Value::Null))
}

fn compare(op: &str, a: &Value, b: &Value) -> Result<bool, String> {
    let ord = match (a, b) {
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => {
            let (Some(x), Some(y)) = (num(a), num(b)) else {
                return Err(format!(
                    "Cannot apply [{op}] operation to types [{}] and [{}].",
                    type_name(a),
                    type_name(b)
                ));
            };
            x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
        }
    };
    Ok(match op {
        "<" => ord.is_lt(),
        ">" => ord.is_gt(),
        "<=" => ord.is_le(),
        _ => ord.is_ge(),
    })
}

fn equals(a: &Value, b: &Value) -> bool {
    match (num(a), num(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// A place a value can be written to: a variable and a path under it.
enum Seg {
    Key(String),
    Idx(i64),
}

impl Env {
    fn get_var(&self, name: &str) -> Result<Value, String> {
        for s in self.scopes.iter().rev() {
            if let Some(v) = s.get(name) {
                return Ok(v.clone());
            }
        }
        Err(format!("cannot resolve symbol [{name}]"))
    }

    fn var_mut(&mut self, name: &str) -> Result<&mut Value, String> {
        for s in self.scopes.iter_mut().rev() {
            if let Some(v) = s.get_mut(name) {
                return Ok(v);
            }
        }
        Err(format!("cannot resolve symbol [{name}]"))
    }

    fn place(&mut self, e: &Expr) -> Result<(String, Vec<Seg>), String> {
        match e {
            Expr::Var(n) => Ok((n.clone(), Vec::new())),
            Expr::Field(base, f, _) => {
                let (n, mut p) = self.place(base)?;
                p.push(Seg::Key(f.clone()));
                Ok((n, p))
            }
            Expr::Index(base, idx) => {
                let i = self.eval(idx)?;
                let (n, mut p) = self.place(base)?;
                p.push(match i {
                    Value::String(s) => Seg::Key(s),
                    other => Seg::Idx(other.as_i64().ok_or("index must be an integer")?),
                });
                Ok((n, p))
            }
            _ => Err("invalid assignment target".into()),
        }
    }

    fn write(&mut self, target: &Expr, v: Value) -> Result<(), String> {
        let (name, path) = self.place(target)?;
        let mut cur = self.var_mut(&name)?;
        for (k, seg) in path.iter().enumerate() {
            let last = k + 1 == path.len();
            cur = match (cur, seg) {
                (Value::Object(m), Seg::Key(key)) => {
                    if last {
                        m.insert(key.clone(), v);
                        return Ok(());
                    }
                    match m.get_mut(key) {
                        Some(next) if !next.is_null() => next,
                        _ => {
                            return Err(format!(
                                "Cannot invoke \"Object.getClass()\" because \"{key}\" is null"
                            ));
                        }
                    }
                }
                (Value::Array(a), Seg::Idx(i)) => {
                    let n = a.len() as i64;
                    let i = if *i < 0 { *i + n } else { *i };
                    if i < 0 || i >= n {
                        return Err(format!("Index {i} out of bounds for length {n}"));
                    }
                    if last {
                        a[i as usize] = v;
                        return Ok(());
                    }
                    &mut a[i as usize]
                }
                (Value::Null, _) => {
                    return Err("Cannot invoke \"Object.getClass()\" because value is null".into());
                }
                (other, _) => {
                    return Err(format!("Illegal list shortcut value [{}].", type_name(other)));
                }
            };
        }
        *cur = v;
        Ok(())
    }

    fn call_method(
        &mut self,
        target: &Expr,
        recv: Value,
        m: &str,
        args: Vec<Value>,
        lambdas: &[Expr],
    ) -> Result<Value, String> {
        let arg = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
        let mutate = |env: &mut Env, v: Value| env.write(target, v);
        if recv.is_null() {
            return Err(format!("Cannot invoke \"{m}()\" because value is null"));
        }
        match (&recv, m) {
            (Value::String(s), _) => {
                let i = |k: usize| arg(k).as_i64().unwrap_or(0).max(0) as usize;
                let chars: Vec<char> = s.chars().collect();
                return Ok(match m {
                    "toUpperCase" => json!(s.to_uppercase()),
                    "toLowerCase" => json!(s.to_lowercase()),
                    "trim" => json!(s.trim()),
                    "length" => json!(chars.len()),
                    "isEmpty" => json!(s.is_empty()),
                    "contains" => json!(s.contains(&to_string(&arg(0)))),
                    "startsWith" => json!(s.starts_with(&to_string(&arg(0)))),
                    "endsWith" => json!(s.ends_with(&to_string(&arg(0)))),
                    "equals" => json!(recv == arg(0)),
                    "equalsIgnoreCase" => json!(s.eq_ignore_ascii_case(&to_string(&arg(0)))),
                    "indexOf" => json!(
                        s.find(&to_string(&arg(0))).map_or(-1, |b| s[..b].chars().count() as i64)
                    ),
                    "charAt" => json!(chars.get(i(0)).map(|c| c.to_string())),
                    "replace" => json!(s.replace(&to_string(&arg(0)), &to_string(&arg(1)))),
                    "substring" => {
                        let (a, b) = (i(0), if args.len() > 1 { i(1) } else { chars.len() });
                        if a > b || b > chars.len() {
                            return Err(format!("begin {a}, end {b}, length {}", chars.len()));
                        }
                        json!(chars[a..b].iter().collect::<String>())
                    }
                    "splitOnToken" | "split" => {
                        json!(s.split(&to_string(&arg(0))).collect::<Vec<_>>())
                    }
                    "toString" => recv.clone(),
                    "hashCode" => json!(
                        s.bytes().fold(0i32, |h, b| h.wrapping_mul(31).wrapping_add(b as i32))
                    ),
                    _ => {
                        return Err(format!(
                            "dynamic method [java.lang.String, {m}/{}] not found",
                            args.len()
                        ));
                    }
                });
            }
            (Value::Array(a), _) => {
                let mut a = a.clone();
                let out = match m {
                    "add" => {
                        if args.len() == 2 {
                            let i = arg(0).as_i64().unwrap_or(0) as usize;
                            if i > a.len() {
                                return Err(format!("Index: {i}, Size: {}", a.len()));
                            }
                            a.insert(i, arg(1));
                        } else {
                            a.push(arg(0));
                        }
                        mutate(self, Value::Array(a))?;
                        return Ok(json!(true));
                    }
                    "addAll" => {
                        a.extend(arg(0).as_array().cloned().unwrap_or_default());
                        mutate(self, Value::Array(a))?;
                        return Ok(json!(true));
                    }
                    "remove" => {
                        // remove(int index) on a List
                        let Some(i) = arg(0).as_i64() else {
                            let before = a.len();
                            if let Some(p) = a.iter().position(|x| equals(x, &arg(0))) {
                                a.remove(p);
                            }
                            let changed = a.len() != before;
                            mutate(self, Value::Array(a))?;
                            return Ok(json!(changed));
                        };
                        if i < 0 || i as usize >= a.len() {
                            return Err(format!("Index {i} out of bounds for length {}", a.len()));
                        }
                        let removed = a.remove(i as usize);
                        mutate(self, Value::Array(a))?;
                        return Ok(removed);
                    }
                    "removeIf" => {
                        let mut keep = Vec::new();
                        for x in a {
                            if !truthy(&self.apply(lambdas.first(), vec![x.clone()])?)? {
                                keep.push(x);
                            }
                        }
                        mutate(self, Value::Array(keep))?;
                        return Ok(json!(true));
                    }
                    "clear" => {
                        mutate(self, json!([]))?;
                        return Ok(Value::Null);
                    }
                    "set" => {
                        let i = arg(0).as_i64().unwrap_or(-1);
                        if i < 0 || i as usize >= a.len() {
                            return Err(format!("Index {i} out of bounds for length {}", a.len()));
                        }
                        let old = std::mem::replace(&mut a[i as usize], arg(1));
                        mutate(self, Value::Array(a))?;
                        return Ok(old);
                    }
                    "sort" => {
                        a.sort_by(|x, y| match (x, y) {
                            (Value::String(p), Value::String(q)) => p.cmp(q),
                            _ => num(x).partial_cmp(&num(y)).unwrap_or(std::cmp::Ordering::Equal),
                        });
                        mutate(self, Value::Array(a))?;
                        return Ok(Value::Null);
                    }
                    "size" | "length" => json!(a.len()),
                    "isEmpty" => json!(a.is_empty()),
                    "contains" => json!(a.iter().any(|x| equals(x, &arg(0)))),
                    "indexOf" => {
                        json!(a.iter().position(|x| equals(x, &arg(0))).map_or(-1, |p| p as i64))
                    }
                    "get" => {
                        let i = arg(0).as_i64().unwrap_or(-1);
                        if i < 0 || i as usize >= a.len() {
                            return Err(format!("Index {i} out of bounds for length {}", a.len()));
                        }
                        a[i as usize].clone()
                    }
                    "join" | "toString" => json!(to_string(&Value::Array(a))),
                    _ => {
                        return Err(format!(
                            "dynamic method [java.util.ArrayList, {m}/{}] not found",
                            args.len()
                        ));
                    }
                };
                return Ok(out);
            }
            (Value::Object(map), _) => {
                let mut map = map.clone();
                let key = to_string(&arg(0));
                let out = match m {
                    "put" => {
                        let old = map.insert(key, arg(1)).unwrap_or(Value::Null);
                        mutate(self, Value::Object(map))?;
                        return Ok(old);
                    }
                    "putIfAbsent" => {
                        let old = map.get(&key).cloned();
                        if old.is_none() {
                            map.insert(key, arg(1));
                            mutate(self, Value::Object(map))?;
                        }
                        return Ok(old.unwrap_or(Value::Null));
                    }
                    "remove" => {
                        let old = map.remove(&key).unwrap_or(Value::Null);
                        mutate(self, Value::Object(map))?;
                        return Ok(old);
                    }
                    "putAll" => {
                        if let Value::Object(o) = arg(0) {
                            map.extend(o);
                        }
                        mutate(self, Value::Object(map))?;
                        return Ok(Value::Null);
                    }
                    "clear" => {
                        mutate(self, json!({}))?;
                        return Ok(Value::Null);
                    }
                    "get" => map.get(&key).cloned().unwrap_or(Value::Null),
                    "getOrDefault" => map.get(&key).cloned().unwrap_or(arg(1)),
                    "containsKey" => json!(map.contains_key(&key)),
                    "containsValue" => json!(map.values().any(|v| equals(v, &arg(0)))),
                    "size" => json!(map.len()),
                    "isEmpty" => json!(map.is_empty()),
                    "keySet" => json!(map.keys().cloned().collect::<Vec<_>>()),
                    "values" => json!(map.values().cloned().collect::<Vec<_>>()),
                    _ => {
                        return Err(format!(
                            "dynamic method [java.util.HashMap, {m}/{}] not found",
                            args.len()
                        ));
                    }
                };
                return Ok(out);
            }
            _ => {}
        }
        match m {
            "toString" => Ok(json!(to_string(&recv))),
            "intValue" | "longValue" => Ok(json!(num(&recv).unwrap_or(0.0) as i64)),
            "doubleValue" => Ok(json!(num(&recv).unwrap_or(0.0))),
            "equals" => Ok(json!(equals(&recv, &arg(0)))),
            _ => {
                Err(format!("dynamic method [{}, {m}/{}] not found", type_name(&recv), args.len()))
            }
        }
    }

    fn apply(&mut self, f: Option<&Expr>, args: Vec<Value>) -> Result<Value, String> {
        let Some(Expr::Lambda(names, body)) = f else { return Err("expected a lambda".into()) };
        let mut scope = Map::new();
        for (n, a) in names.iter().zip(args) {
            scope.insert(n.clone(), a);
        }
        self.scopes.push(scope);
        let r = self.eval(body);
        self.scopes.pop();
        r
    }

    fn eval(&mut self, e: &Expr) -> Result<Value, String> {
        self.steps += 1;
        if self.steps > 1_000_000 {
            return Err(
                "The maximum number of statements that can be executed in a loop has been reached."
                    .into(),
            );
        }
        Ok(match e {
            Expr::Lit(v) => v.clone(),
            Expr::Var(n) => self.get_var(n)?,
            Expr::Field(base, f, safe) => {
                let b = self.eval(base)?;
                match &b {
                    Value::Object(m) => m.get(f).cloned().unwrap_or(Value::Null),
                    Value::Null if *safe => Value::Null,
                    Value::Null => {
                        return Err(format!(
                            "Cannot invoke \"Object.getClass()\" because value is null [{f}]"
                        ));
                    }
                    Value::Array(a) if f == "length" => json!(a.len()),
                    other => {
                        return Err(format!(
                            "Illegal list shortcut value [{f}] on {}",
                            type_name(other)
                        ));
                    }
                }
            }
            Expr::Index(base, idx) => {
                let (b, i) = (self.eval(base)?, self.eval(idx)?);
                match (&b, &i) {
                    (Value::Object(m), _) => m.get(&to_string(&i)).cloned().unwrap_or(Value::Null),
                    (Value::Array(a), Value::Number(n)) => {
                        let n = n.as_i64().unwrap_or(0);
                        let len = a.len() as i64;
                        let k = if n < 0 { n + len } else { n };
                        if k < 0 || k >= len {
                            return Err(format!("Index {n} out of bounds for length {len}"));
                        }
                        a[k as usize].clone()
                    }
                    (Value::Null, _) => {
                        return Err(
                            "Cannot invoke \"Object.getClass()\" because value is null".into()
                        );
                    }
                    _ => return Err("Illegal list shortcut".into()),
                }
            }
            Expr::Call(recv, m, args) => {
                let r = self.eval(recv)?;
                let mut vals = Vec::new();
                let mut lambdas = Vec::new();
                for a in args {
                    if matches!(a, Expr::Lambda(..)) {
                        lambdas.push(a.clone());
                    } else {
                        vals.push(self.eval(a)?);
                    }
                }
                self.call_method(recv, r, m, vals, &lambdas)?
            }
            Expr::Static(class, m, args) => {
                let vals: Vec<Value> =
                    args.iter().map(|a| self.eval(a)).collect::<Result<_, _>>()?;
                let f = |i: usize| vals.get(i).and_then(num).unwrap_or(0.0);
                let keep_int = vals.iter().all(is_int);
                let n = |x: f64| -> Value {
                    if keep_int {
                        json!(x as i64)
                    } else {
                        Number::from_f64(x).map(Value::Number).unwrap_or(Value::Null)
                    }
                };
                match (class.as_str(), m.as_str()) {
                    ("Math", "max") => n(f(0).max(f(1))),
                    ("Math", "min") => n(f(0).min(f(1))),
                    ("Math", "abs") => n(f(0).abs()),
                    ("Math", "floor") => json!(f(0).floor()),
                    ("Math", "ceil") => json!(f(0).ceil()),
                    ("Math", "round") => json!(f(0).round() as i64),
                    ("Math", "sqrt") => json!(f(0).sqrt()),
                    ("Math", "pow") => json!(f(0).powf(f(1))),
                    ("Math", "log") => json!(f(0).ln()),
                    ("Integer" | "Long", "parseInt" | "parseLong" | "valueOf") => {
                        let s = vals.first().map(to_string).unwrap_or_default();
                        json!(
                            s.trim()
                                .parse::<i64>()
                                .map_err(|_| format!("For input string: \"{s}\""))?
                        )
                    }
                    ("Double" | "Float", "parseDouble" | "parseFloat" | "valueOf") => {
                        let s = vals.first().map(to_string).unwrap_or_default();
                        json!(
                            s.trim()
                                .parse::<f64>()
                                .map_err(|_| format!("For input string: \"{s}\""))?
                        )
                    }
                    ("String", "valueOf") => json!(vals.first().map(to_string).unwrap_or_default()),
                    ("cast", "long") => json!(f(0) as i64),
                    ("cast", "double") => json!(f(0)),
                    ("instanceof", ty) => {
                        let v = vals.first().cloned().unwrap_or(Value::Null);
                        json!(match ty {
                            "String" => v.is_string(),
                            "List" | "ArrayList" | "Collection" => v.is_array(),
                            "Map" | "HashMap" => v.is_object(),
                            "Integer" | "Long" | "int" | "long" => is_int(&v),
                            "Number" => v.is_number(),
                            "Double" | "Float" => v.is_f64(),
                            "Boolean" => v.is_boolean(),
                            _ => false,
                        })
                    }
                    _ => return Err(format!("cannot resolve symbol [{class}.{m}]")),
                }
            }
            Expr::New(ty, args) => match ty.as_str() {
                "ArrayList" | "LinkedList" | "HashSet" | "TreeSet" => match args.first() {
                    Some(a) => {
                        let v = self.eval(a)?;
                        if v.is_array() { v } else { json!([]) }
                    }
                    None => json!([]),
                },
                "HashMap" | "TreeMap" | "LinkedHashMap" => json!({}),
                "String" => json!(
                    args.first()
                        .map(|a| self.eval(a))
                        .transpose()?
                        .map(|v| to_string(&v))
                        .unwrap_or_default()
                ),
                _ => return Err(format!("cannot resolve type [{ty}]")),
            },
            Expr::List(items) => {
                Value::Array(items.iter().map(|i| self.eval(i)).collect::<Result<_, _>>()?)
            }
            Expr::MapLit(pairs) => {
                let mut m = Map::new();
                for (k, v) in pairs {
                    let k = to_string(&self.eval(k)?);
                    m.insert(k, self.eval(v)?);
                }
                Value::Object(m)
            }
            Expr::Unary(op, x) => {
                let v = self.eval(x)?;
                match *op {
                    "!" => json!(!truthy(&v)?),
                    "-" => arith("-", &json!(0), &v)?,
                    _ => v,
                }
            }
            Expr::Binary(op, a, b) => match *op {
                "&&" => json!(truthy(&self.eval(a)?)? && truthy(&self.eval(b)?)?),
                "||" => json!(truthy(&self.eval(a)?)? || truthy(&self.eval(b)?)?),
                "==" | "===" => json!(equals(&self.eval(a)?, &self.eval(b)?)),
                "!=" | "!==" => json!(!equals(&self.eval(a)?, &self.eval(b)?)),
                "<" | ">" | "<=" | ">=" => {
                    let (x, y) = (self.eval(a)?, self.eval(b)?);
                    json!(compare(op, &x, &y)?)
                }
                _ => {
                    let (x, y) = (self.eval(a)?, self.eval(b)?);
                    arith(op, &x, &y)?
                }
            },
            Expr::Assign(op, target, value) => {
                let v = self.eval(value)?;
                let v = if *op == "=" {
                    v
                } else {
                    let cur = self.eval(target)?;
                    arith(&op[..1], &cur, &v)?
                };
                self.write(target, v.clone())?;
                v
            }
            Expr::IncDec(op, prefix, target) => {
                let cur = self.eval(target)?;
                let next = arith(if *op == "++" { "+" } else { "-" }, &cur, &json!(1))?;
                self.write(target, next.clone())?;
                if *prefix { next } else { cur }
            }
            Expr::Ternary(c, a, b) => {
                if truthy(&self.eval(c)?)? {
                    self.eval(a)?
                } else {
                    self.eval(b)?
                }
            }
            Expr::Lambda(..) => return Err("unexpected lambda".into()),
            Expr::Cast(x) => self.eval(x)?,
        })
    }

    fn run(&mut self, stmts: &[Stmt]) -> Result<Flow, String> {
        for s in stmts {
            match self.exec(s)? {
                Flow::Normal => {}
                other => return Ok(other),
            }
        }
        Ok(Flow::Normal)
    }

    fn scoped(&mut self, stmts: &[Stmt]) -> Result<Flow, String> {
        self.scopes.push(Map::new());
        let r = self.run(stmts);
        self.scopes.pop();
        r
    }

    fn exec(&mut self, s: &Stmt) -> Result<Flow, String> {
        match s {
            Stmt::Expr(e) => {
                self.eval(e)?;
            }
            Stmt::Decl(n, init) => {
                let v = match init {
                    Some(e) => self.eval(e)?,
                    None => Value::Null,
                };
                self.scopes.last_mut().unwrap().insert(n.clone(), v);
            }
            Stmt::If(c, a, b) => {
                let branch = if truthy(&self.eval(c)?)? { a } else { b };
                return self.scoped(branch);
            }
            Stmt::While(c, body) => {
                while truthy(&self.eval(c)?)? {
                    match self.scoped(body)? {
                        Flow::Break => break,
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        _ => {}
                    }
                }
            }
            Stmt::For(init, cond, step, body) => {
                self.scopes.push(Map::new());
                if let Some(i) = init {
                    self.exec(i)?;
                }
                loop {
                    if let Some(c) = cond
                        && !truthy(&self.eval(c)?)?
                    {
                        break;
                    }
                    match self.scoped(body)? {
                        Flow::Break => break,
                        Flow::Return(v) => {
                            self.scopes.pop();
                            return Ok(Flow::Return(v));
                        }
                        _ => {}
                    }
                    if let Some(st) = step {
                        self.eval(st)?;
                    }
                }
                self.scopes.pop();
            }
            Stmt::ForEach(name, iter, body) => {
                let items: Vec<Value> = match self.eval(iter)? {
                    Value::Array(a) => a,
                    Value::Object(m) => m.into_iter().map(|(k, _)| json!(k)).collect(),
                    Value::Null => return Err("Cannot iterate over null".into()),
                    other => return Err(format!("Cannot iterate over {}", type_name(&other))),
                };
                for item in items {
                    self.scopes.push(Map::from_iter([(name.clone(), item)]));
                    let r = self.run(body);
                    self.scopes.pop();
                    match r? {
                        Flow::Break => break,
                        Flow::Return(v) => return Ok(Flow::Return(v)),
                        _ => {}
                    }
                }
            }
            Stmt::Return(e) => {
                let v = match e {
                    Some(e) => self.eval(e)?,
                    None => Value::Null,
                };
                return Ok(Flow::Return(v));
            }
            Stmt::Break => return Ok(Flow::Break),
            Stmt::Continue => return Ok(Flow::Continue),
        }
        Ok(Flow::Normal)
    }
}

impl Script {
    /// Runs the script with `ctx` and `params` bound; returns the final
    /// `ctx` (scripts change it in place).
    pub fn run(&self, ctx: Value, params: Value) -> Result<Value, String> {
        let mut env = Env {
            scopes: vec![Map::from_iter([
                ("ctx".to_string(), ctx),
                ("params".to_string(), params),
            ])],
            steps: 0,
        };
        env.run(&self.stmts)?;
        Ok(env.scopes.swap_remove(0).remove("ctx").unwrap_or(Value::Null))
    }
}

impl Script {
    /// Runs a scoring/value script (`script_score`, script fields) with
    /// `vars` bound (`doc`, `_score`, `params`, ...): its `return` value,
    /// or the value of its last expression (`doc['n'].value * 2`).
    pub fn value(&self, vars: Map<String, Value>) -> Result<Value, String> {
        let mut env = Env { scopes: vec![vars], steps: 0 };
        let (last, init) = match self.stmts.split_last() {
            Some((Stmt::Expr(e), init)) => (Some(e), init),
            _ => (None, &self.stmts[..]),
        };
        if let Flow::Return(v) = env.run(init)? {
            return Ok(v);
        }
        match last {
            Some(e) => env.eval(e),
            None => Ok(Value::Null),
        }
    }
}

/// A request's `script` (a string, or `{source, params, lang}`) as
/// (source, params).
pub fn script_parts(script: &Value) -> Result<(String, Value), String> {
    match script {
        Value::String(s) => Ok((s.clone(), json!({}))),
        Value::Object(o) => {
            if let Some(lang) = o.get("lang").and_then(Value::as_str)
                && lang != "painless"
            {
                return Err(format!("script_lang not supported [{lang}]"));
            }
            if o.contains_key("id") {
                return Err("stored scripts are not supported".into());
            }
            let src = o.get("source").or_else(|| o.get("inline")).and_then(Value::as_str).ok_or(
                "must specify either [source] for an inline script or [id] for a stored script",
            )?;
            Ok((src.to_string(), o.get("params").cloned().unwrap_or_else(|| json!({}))))
        }
        _ => Err("[script] must be a string or an object".into()),
    }
}

/// The error Elasticsearch returns for a script that failed to compile or
/// run during an update (400 `illegal_argument_exception`, caused by a
/// `script_exception`).
pub fn script_error(source: &str, reason: &str, compile: bool) -> Value {
    let cause = json!({
        "type": "script_exception",
        "reason": if compile { "compile error" } else { "runtime error" },
        "script_stack": [],
        "script": source,
        "lang": "painless",
        "caused_by": {"type": "illegal_argument_exception", "reason": reason},
    });
    json!({
        "error": {
            "root_cause": [{"type": "illegal_argument_exception", "reason": "failed to execute script"}],
            "type": "illegal_argument_exception",
            "reason": "failed to execute script",
            "caused_by": cause,
        },
        "status": 400,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(src: &str, source: Value, params: Value) -> Result<Value, String> {
        compile(src)?.run(json!({"_source": source, "op": "index"}), params)
    }

    #[test]
    fn updates_fields() {
        let ctx = run(
            "ctx._source.n += params.k; ctx._source.s = 'x' + ctx._source.n",
            json!({"n": 1}),
            json!({"k": 2}),
        )
        .unwrap();
        assert_eq!(ctx["_source"], json!({"n": 3, "s": "x3"}));
    }

    #[test]
    fn integer_division_and_control_flow() {
        let src = "int total = 0; for (def x : ctx._source.xs) { if (x % 2 == 0) { continue } total += x } ctx._source.t = total / 2;";
        let ctx = run(src, json!({"xs": [1, 2, 3, 4, 5]}), json!({})).unwrap();
        assert_eq!(ctx["_source"]["t"], json!(4));
    }

    #[test]
    fn list_and_map_methods() {
        let src = "ctx._source.tags.add('c'); ctx._source.tags.removeIf(t -> t == 'a'); ctx._source.m.put('k', ctx._source.tags.size()); ctx._source.remove('gone');";
        let ctx = run(src, json!({"tags": ["a", "b"], "m": {}, "gone": 1}), json!({})).unwrap();
        assert_eq!(ctx["_source"], json!({"tags": ["b", "c"], "m": {"k": 2}}));
    }

    #[test]
    fn null_deref_and_syntax_errors() {
        assert!(run("ctx._source.missing.foo = 1", json!({}), json!({})).is_err());
        assert!(compile("this is not painless (((").is_err());
    }
}
