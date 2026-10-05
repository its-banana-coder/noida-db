//! User-defined functions, procedures, DO blocks and triggers: PL/pgSQL
//! (a focused, commonly used subset) and LANGUAGE sql.
//!
//! The SQL parser doesn't handle all of this DDL (DO blocks, procedures,
//! trigger arguments), so these statements are parsed here: `rewrite`
//! turns each into `CALL noida_plpgsql('<statement text>')`, which the
//! engine hands back to `ddl`. A function body is parsed by `Parser` below
//! into `Stmt`s; every SQL expression or statement inside it runs through
//! the engine's normal statement path (`engine::run_one`) against the same
//! transaction, with PL/pgSQL variables passed as typed `$n` parameters.

use std::collections::HashMap;

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer, Whitespace};

use super::binder::{Binder, SessionInfo};
use super::catalog::{Function, Row, Trigger};
use super::error::{PgError, PgResult, code};
use super::exec::Ctx;
use super::types::{self, Base, Type, Value};

pub const CALL_NAME: &str = "noida_plpgsql";

/// Deepest nesting of function and trigger calls (Postgres stops runaway
/// recursion with "stack depth limit exceeded").
const MAX_DEPTH: usize = 64;

thread_local! {
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn syntax(near: &str) -> PgError {
    PgError::new(code::SYNTAX_ERROR, format!("syntax error at or near \"{near}\""))
}

// ---------------------------------------------------------------------------
// Tokens with their source positions.

#[derive(Clone, Debug)]
struct Tok {
    t: Token,
    start: usize,
    end: usize,
}

fn tokenize(src: &str) -> PgResult<Vec<Tok>> {
    let toks = Tokenizer::new(&PostgreSqlDialect {}, src)
        .tokenize_with_location()
        .map_err(|e| PgError::new(code::SYNTAX_ERROR, format!("syntax error: {e}")))?;
    let mut line_starts = vec![0usize];
    for (i, c) in src.char_indices() {
        if c == '\n' {
            line_starts.push(i + 1);
        }
    }
    let offset = |loc: Location| -> usize {
        if loc.line == 0 {
            return src.len();
        }
        let start = line_starts.get(loc.line as usize - 1).copied().unwrap_or(src.len());
        src[start..]
            .char_indices()
            .nth(loc.column as usize - 1)
            .map_or(src.len(), |(i, _)| start + i)
    };
    Ok(toks
        .into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .map(|t| Tok { start: offset(t.span.start), end: offset(t.span.end), t: t.token })
        .collect())
}

fn is_comment(t: &Token) -> bool {
    matches!(
        t,
        Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_))
    )
}

fn word_of(t: &Token) -> Option<String> {
    match t {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.to_ascii_lowercase()),
        _ => None,
    }
}

/// An identifier's name: unquoted ones fold to lower case.
fn ident_of(t: &Token) -> Option<String> {
    match t {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.to_ascii_lowercase()),
        Token::Word(w) => Some(w.value.clone()),
        _ => None,
    }
}

/// The text of a string literal token (`'...'`, `$$...$$`, `E'...'`).
fn string_of(t: &Token) -> Option<String> {
    match t {
        Token::SingleQuotedString(s) | Token::EscapedStringLiteral(s) => Some(s.clone()),
        Token::DollarQuotedString(d) => Some(d.value.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Rewriting the DDL statements the SQL parser can't read.

/// Rewrites every function/procedure/trigger/DO/CALL statement in `sql`
/// to `CALL noida_plpgsql('<its text>')`.
pub fn rewrite(sql: &str) -> PgResult<Option<String>> {
    let lower = sql.to_ascii_lowercase();
    if !["function", "procedure", "trigger", "do", "call"].iter().any(|k| lower.contains(k)) {
        return Ok(None);
    }
    let Ok(toks) = tokenize(sql) else { return Ok(None) };
    let mut out = String::new();
    let mut changed = false;
    let mut stmt_start = 0usize;
    let mut words: Vec<String> = vec![];
    let emit = |range: (usize, usize), words: &mut Vec<String>, out: &mut String| -> bool {
        let text = &sql[range.0..range.1];
        let w: Vec<&str> = words.iter().map(String::as_str).collect();
        let ours = matches!(
            w.as_slice(),
            ["create", "function", ..]
                | ["create", "procedure", ..]
                | ["create", "or", "replace", ..]
                | ["create", "trigger", ..]
                | ["create", "constraint", "trigger", ..]
                | ["do", ..]
                | ["drop", "function", ..]
                | ["drop", "procedure", ..]
                | ["drop", "trigger", ..]
                | ["drop", "routine", ..]
        ) || (w.first() == Some(&"call")
            && !text.contains(super::seqddl::CALL_NAME)
            && !text.contains(super::refresh::CALL_NAME)
            && !text.contains(CALL_NAME));
        // `CREATE OR REPLACE VIEW` and friends stay with the SQL parser.
        let ours = ours
            && !(w.len() >= 4
                && w[0] == "create"
                && w[1] == "or"
                && !matches!(w[3], "function" | "procedure" | "trigger"));
        words.clear();
        if ours {
            out.push_str(&format!("CALL {CALL_NAME}('{}')", text.trim().replace('\'', "''")));
        } else {
            out.push_str(text);
        }
        ours
    };
    for t in &toks {
        if is_comment(&t.t) {
            continue;
        }
        match &t.t {
            Token::SemiColon => {
                changed |= emit((stmt_start, t.start), &mut words, &mut out);
                out.push(';');
                stmt_start = t.end;
            }
            other => {
                if words.len() < 4
                    && let Some(w) = word_of(other)
                {
                    words.push(w);
                } else if words.len() < 4 {
                    words.push(String::new());
                }
            }
        }
    }
    changed |= emit((stmt_start, sql.len()), &mut words, &mut out);
    Ok(changed.then_some(out))
}

/// A cursor over tokens.
struct Cur<'s> {
    toks: Vec<Tok>,
    i: usize,
    src: &'s str,
}

impl<'s> Cur<'s> {
    fn new(src: &'s str) -> PgResult<Self> {
        let toks = tokenize(src)?.into_iter().filter(|t| !is_comment(&t.t)).collect();
        Ok(Cur { toks, i: 0, src })
    }
    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.i).map(|t| &t.t)
    }
    fn peek_at(&self, k: usize) -> Option<&Token> {
        self.toks.get(self.i + k).map(|t| &t.t)
    }
    fn word(&self) -> Option<String> {
        self.peek().and_then(word_of)
    }
    fn word_at(&self, k: usize) -> Option<String> {
        self.peek_at(k).and_then(word_of)
    }
    fn eat(&mut self, kw: &str) -> bool {
        if self.word().as_deref() == Some(kw) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn eat_tok(&mut self, t: &Token) -> bool {
        if self.peek() == Some(t) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, kw: &str) -> PgResult<()> {
        if self.eat(kw) { Ok(()) } else { Err(syntax(&self.near())) }
    }
    fn expect_tok(&mut self, t: Token) -> PgResult<()> {
        if self.eat_tok(&t) { Ok(()) } else { Err(syntax(&self.near())) }
    }
    fn near(&self) -> String {
        self.peek().map_or("end of input".into(), |t| t.to_string())
    }
    fn at_end(&self) -> bool {
        self.i >= self.toks.len()
    }
    fn ident(&mut self) -> PgResult<String> {
        match self.peek().and_then(ident_of) {
            Some(w) => {
                self.i += 1;
                Ok(w)
            }
            None => Err(syntax(&self.near())),
        }
    }
    /// A possibly qualified name, as its parts.
    fn qualified(&mut self) -> PgResult<Vec<String>> {
        let mut parts = vec![self.ident()?];
        while self.eat_tok(&Token::Period) {
            parts.push(self.ident()?);
        }
        Ok(parts)
    }
    /// Source text from token `from` up to (not including) token `to`.
    fn text(&self, from: usize, to: usize) -> String {
        if from >= to || from >= self.toks.len() {
            return String::new();
        }
        let s = self.toks[from].start;
        let e = self.toks.get(to - 1).map_or(self.src.len(), |t| t.end);
        self.src[s..e].trim().to_string()
    }
    /// Advances to the next top-level token that is `;` or one of the
    /// `stop` words (or a stop token) and returns the text skipped.
    fn until(&mut self, stop_words: &[&str], stop_toks: &[Token]) -> String {
        let start = self.i;
        let mut depth = 0i32;
        while let Some(t) = self.peek() {
            match t {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => {
                    if depth == 0 && stop_toks.contains(t) {
                        break;
                    }
                    depth -= 1;
                }
                Token::SemiColon if depth == 0 => break,
                _ if depth == 0 && stop_toks.contains(t) => break,
                _ if depth == 0 && word_of(t).is_some_and(|w| stop_words.contains(&w.as_str())) => {
                    break;
                }
                _ => {}
            }
            self.i += 1;
        }
        self.text(start, self.i)
    }
}

// ---------------------------------------------------------------------------
// DDL: CREATE FUNCTION / PROCEDURE / TRIGGER, DROP, DO, CALL.

/// Resolves a type name (`int`, `numeric(10,2)`, `text[]`, `trigger`).
fn parse_type(text: &str, db: &super::catalog::DbState, info: &SessionInfo) -> PgResult<Type> {
    let t = text.trim().to_ascii_lowercase();
    match t.as_str() {
        "trigger" => return Ok(Type::of(Base::Trigger)),
        "void" => return Ok(Type::of(Base::Void)),
        "record" => return Ok(Type::of(Base::Record)),
        _ => {}
    }
    // `table.col%TYPE` / `table%ROWTYPE`.
    if let Some(base) = t.strip_suffix("%type") {
        let parts: Vec<&str> = base.split('.').collect();
        if let [tbl, col] = parts.as_slice()
            && let Some(table) = db.find_table_by_name(tbl, &info.search_path)
            && let Some(i) = table.col_index(col)
        {
            return Ok(table.columns[i].ty);
        }
        return Err(PgError::new(
            code::UNDEFINED_OBJECT,
            format!("type \"{text}\" does not exist"),
        ));
    }
    if t.ends_with("%rowtype") {
        return Ok(Type::of(Base::Record));
    }
    let dialect = PostgreSqlDialect {};
    let mut p =
        sqlparser::parser::Parser::new(&dialect).try_with_sql(text).map_err(|_| syntax(text))?;
    let dt = p.parse_data_type().map_err(|_| syntax(text))?;
    let b = Binder::new(db, info, &[]);
    Ok(b.data_type(&dt)?.0)
}

/// A type's SQL name, for casts.
fn type_sql(ty: Type) -> String {
    let name = ty.base.info().map_or("text", |i| i.name);
    if ty.array { format!("{name}[]") } else { name.to_string() }
}

/// Runs one of the statements `rewrite` turned into a CALL.
pub fn ddl(ctx: &mut Ctx, info: &SessionInfo, text: &str) -> PgResult<String> {
    let mut c = Cur::new(text)?;
    match c.word().as_deref() {
        Some("create") => {
            c.i += 1;
            let replace = c.eat("or") && {
                c.expect("replace")?;
                true
            };
            if c.eat("function") {
                create_function(ctx, info, &mut c, replace, false)
            } else if c.eat("procedure") {
                create_function(ctx, info, &mut c, replace, true)
            } else {
                c.eat("constraint");
                c.expect("trigger")?;
                create_trigger(ctx, info, &mut c, replace)
            }
        }
        Some("drop") => {
            c.i += 1;
            if c.eat("trigger") {
                drop_trigger(ctx, info, &mut c)
            } else {
                let proc = c.word().as_deref() == Some("procedure");
                c.i += 1;
                drop_function(ctx, info, &mut c, proc)
            }
        }
        Some("do") => {
            c.i += 1;
            let mut lang = "plpgsql".to_string();
            let mut body = None;
            while !c.at_end() {
                if c.eat("language") {
                    lang = c.ident()?;
                } else if let Some(s) = c.peek().and_then(string_of) {
                    body = Some(s);
                    c.i += 1;
                } else {
                    return Err(syntax(&c.near()));
                }
            }
            if lang != "plpgsql" {
                return Err(PgError::new(
                    code::UNDEFINED_OBJECT,
                    format!("language \"{lang}\" does not exist"),
                ));
            }
            let body = body.ok_or_else(|| syntax("DO"))?;
            let block = Parser::parse_body(&body)?;
            let func = Function { name: "inline_code_block".into(), ..Function::default() };
            let mut frame = Frame::new(&func);
            enter()?;
            let r = exec_block(ctx, info, &mut frame, &block);
            leave();
            r?;
            Ok("DO".into())
        }
        Some("call") => {
            c.i += 1;
            let name = c.qualified()?;
            c.expect_tok(Token::LParen)?;
            let args_text = c.until(&[], &[Token::RParen]);
            let args = if args_text.is_empty() {
                vec![]
            } else {
                let sql = format!("SELECT {args_text}");
                run_sql(ctx, info, &sql, &[], &[])?.rows.into_iter().next().unwrap_or_default()
            };
            let name = name.last().cloned().unwrap_or_default();
            let f = lookup(ctx, &name, args.len(), true)?;
            call_function(ctx, info, &f, args)?;
            Ok("CALL".into())
        }
        _ => Err(syntax(&c.near())),
    }
}

/// The user function `name` callable with `nargs` arguments.
pub fn lookup(ctx: &Ctx, name: &str, nargs: usize, procedure: bool) -> PgResult<Function> {
    ctx.db
        .functions
        .values()
        .find(|f| {
            f.name == name
                && f.procedure == procedure
                && nargs <= f.arg_types.len()
                && nargs + f.arg_defaults.iter().filter(|d| d.is_some()).count()
                    >= f.arg_types.len()
        })
        .cloned()
        .ok_or_else(|| {
            let what = if procedure { "procedure" } else { "function" };
            PgError::new(
                code::UNDEFINED_FUNCTION,
                format!("{what} {name}({}) does not exist", vec!["unknown"; nargs].join(", ")),
            )
        })
}

fn create_function(
    ctx: &mut Ctx,
    info: &SessionInfo,
    c: &mut Cur,
    replace: bool,
    procedure: bool,
) -> PgResult<String> {
    let name_parts = c.qualified()?;
    let name = name_parts.last().cloned().unwrap_or_default();
    c.expect_tok(Token::LParen)?;
    let mut arg_names = vec![];
    let mut arg_types = vec![];
    let mut arg_defaults = vec![];
    if !c.eat_tok(&Token::RParen) {
        loop {
            // [IN] [name] type [DEFAULT|= expr]
            if c.eat("out") || c.eat("inout") || c.eat("variadic") {
                return Err(PgError::new(
                    code::FEATURE_NOT_SUPPORTED,
                    "OUT, INOUT and VARIADIC parameters are not supported",
                ));
            }
            c.eat("in");
            let spec = c.until(&["default"], &[Token::Comma, Token::RParen, Token::Eq]);
            let default = if c.eat("default") || c.eat_tok(&Token::Eq) {
                Some(c.until(&[], &[Token::Comma, Token::RParen]))
            } else {
                None
            };
            // `name type`, unless the whole spec is a type.
            let (pname, ty) = match spec.split_once(char::is_whitespace) {
                Some((first, rest))
                    if !matches!(
                        first.to_ascii_lowercase().as_str(),
                        "double" | "character" | "timestamp" | "time" | "bit" | "char" | "varchar"
                    ) && parse_type(rest, ctx.db, info).is_ok() =>
                {
                    (first.trim_matches('"').to_ascii_lowercase(), parse_type(rest, ctx.db, info)?)
                }
                _ => (String::new(), parse_type(&spec, ctx.db, info)?),
            };
            arg_names.push(pname);
            arg_types.push(ty);
            arg_defaults.push(default);
            if c.eat_tok(&Token::RParen) {
                break;
            }
            c.expect_tok(Token::Comma)?;
        }
    }
    let mut f = Function {
        name: name.clone(),
        schema: super::catalog::PUBLIC_NS,
        arg_names,
        arg_types,
        arg_defaults,
        ret: Type::of(Base::Void),
        procedure,
        language: String::new(),
        volatility: 'v',
        ..Function::default()
    };
    while !c.at_end() {
        if c.eat("returns") {
            if c.eat("setof") {
                f.returns_set = true;
                f.ret = parse_type(&c.until(LANG_WORDS, &[]), ctx.db, info)?;
            } else if c.eat("table") {
                c.expect_tok(Token::LParen)?;
                f.returns_set = true;
                f.ret = Type::of(Base::Record);
                loop {
                    let col = c.ident()?;
                    let ty = c.until(&[], &[Token::Comma, Token::RParen]);
                    f.out_cols.push((col, parse_type(&ty, ctx.db, info)?));
                    if c.eat_tok(&Token::RParen) {
                        break;
                    }
                    c.expect_tok(Token::Comma)?;
                }
            } else {
                f.ret = parse_type(&c.until(LANG_WORDS, &[]), ctx.db, info)?;
            }
        } else if c.eat("language") {
            f.language = c.ident()?;
        } else if c.eat("as") {
            let s = c.peek().and_then(string_of).ok_or_else(|| syntax(&c.near()))?;
            f.body = s;
            c.i += 1;
        } else if c.eat("immutable") {
            f.volatility = 'i';
        } else if c.eat("stable") {
            f.volatility = 's';
        } else if c.eat("volatile") || c.eat("leakproof") || c.eat("window") {
        } else if c.eat("strict") {
            f.strict = true;
        } else if c.eat("called") {
            c.expect("on")?;
            c.expect("null")?;
            c.expect("input")?;
        } else if c.word().as_deref() == Some("returns") && c.word_at(1).as_deref() == Some("null")
        {
            c.i += 4;
            f.strict = true;
        } else if c.eat("security") || c.eat("parallel") || c.eat("cost") || c.eat("rows") {
            c.i += 1;
        } else if c.eat("not") {
            c.expect("leakproof")?;
        } else if c.eat("set") {
            c.until(LANG_WORDS, &[]);
        } else {
            return Err(syntax(&c.near()));
        }
    }
    if f.language.is_empty() {
        return Err(PgError::new(code::INVALID_FUNCTION_DEFINITION, "no language specified"));
    }
    if f.language != "plpgsql" && f.language != "sql" {
        return Err(PgError::new(
            code::UNDEFINED_OBJECT,
            format!("language \"{}\" does not exist", f.language),
        ));
    }
    if f.body.is_empty() {
        return Err(PgError::new(code::INVALID_FUNCTION_DEFINITION, "no function body specified"));
    }
    // A PL/pgSQL body is checked for syntax when it's created.
    if f.language == "plpgsql" {
        Parser::parse_body(&f.body)?;
    }
    if f.ret.base == Base::Trigger && (f.language != "plpgsql" || !f.arg_types.is_empty()) {
        return Err(PgError::new(
            code::INVALID_FUNCTION_DEFINITION,
            "trigger functions can only be written in PL/pgSQL and take no arguments",
        ));
    }
    let existing = ctx
        .db
        .functions
        .values()
        .find(|g| g.name == f.name && g.arg_types == f.arg_types)
        .map(|g| g.oid);
    let what = if procedure { "PROCEDURE" } else { "FUNCTION" };
    match existing {
        Some(oid) if replace => {
            f.oid = oid;
            ctx.db.functions.insert(oid, f);
        }
        Some(_) => {
            return Err(PgError::new(
                code::DUPLICATE_FUNCTION,
                format!(
                    "{} {name}({}) already exists with same argument types",
                    what.to_lowercase(),
                    f.arg_types.iter().map(|t| type_sql(*t)).collect::<Vec<_>>().join(", ")
                ),
            ));
        }
        None => {
            f.oid = ctx.db.alloc_oid();
            ctx.db.functions.insert(f.oid, f);
        }
    }
    Ok(format!("CREATE {what}"))
}

const LANG_WORDS: &[&str] = &[
    "language",
    "as",
    "immutable",
    "stable",
    "volatile",
    "strict",
    "called",
    "security",
    "parallel",
    "cost",
    "rows",
    "set",
    "leakproof",
    "window",
];

fn drop_function(
    ctx: &mut Ctx,
    info: &SessionInfo,
    c: &mut Cur,
    procedure: bool,
) -> PgResult<String> {
    let if_exists = c.eat("if") && {
        c.expect("exists")?;
        true
    };
    loop {
        let name = c.qualified()?.pop().unwrap_or_default();
        let types: Option<Vec<Type>> = if c.eat_tok(&Token::LParen) {
            let mut tys = vec![];
            if !c.eat_tok(&Token::RParen) {
                loop {
                    let spec = c.until(&[], &[Token::Comma, Token::RParen]);
                    let spec = spec.trim_start_matches("IN ").trim_start_matches("in ");
                    let ty = match spec.split_once(char::is_whitespace) {
                        Some((_, rest))
                            if parse_type(rest, ctx.db, info).is_ok()
                                && parse_type(spec, ctx.db, info).is_err() =>
                        {
                            parse_type(rest, ctx.db, info)?
                        }
                        _ => parse_type(spec, ctx.db, info)?,
                    };
                    tys.push(ty);
                    if c.eat_tok(&Token::RParen) {
                        break;
                    }
                    c.expect_tok(Token::Comma)?;
                }
            }
            Some(tys)
        } else {
            None
        };
        let found: Vec<u32> = ctx
            .db
            .functions
            .values()
            .filter(|f| {
                f.name == name
                    && f.procedure == procedure
                    && types.as_ref().is_none_or(|t| *t == f.arg_types)
            })
            .map(|f| f.oid)
            .collect();
        match found.as_slice() {
            [] if if_exists => {
                ctx.rt.notices.push(notice(&format!(
                    "{} {name}() does not exist, skipping",
                    if procedure { "procedure" } else { "function" }
                )));
            }
            [] => {
                return Err(PgError::new(
                    code::UNDEFINED_FUNCTION,
                    format!(
                        "{} {name}() does not exist",
                        if procedure { "procedure" } else { "function" }
                    ),
                ));
            }
            [oid] => {
                if let Some(t) = ctx.db.triggers.values().find(|t| t.function == *oid)
                    && !c.toks.iter().any(|t| word_of(&t.t).as_deref() == Some("cascade"))
                {
                    return Err(PgError::new(
                        code::DEPENDENT_OBJECTS_STILL_EXIST,
                        format!(
                            "cannot drop function {name}() because other objects depend on it (trigger {} on its table)",
                            t.name
                        ),
                    ));
                }
                let oid = *oid;
                ctx.db.triggers.retain(|_, t| t.function != oid);
                ctx.db.functions.remove(&oid);
            }
            _ => {
                return Err(PgError::new(
                    code::AMBIGUOUS_FUNCTION,
                    format!("function name \"{name}\" is not unique"),
                ));
            }
        }
        if !c.eat_tok(&Token::Comma) {
            break;
        }
    }
    Ok(if procedure { "DROP PROCEDURE" } else { "DROP FUNCTION" }.into())
}

fn create_trigger(
    ctx: &mut Ctx,
    info: &SessionInfo,
    c: &mut Cur,
    replace: bool,
) -> PgResult<String> {
    let name = c.ident()?;
    let timing = if c.eat("before") {
        "BEFORE"
    } else if c.eat("after") {
        "AFTER"
    } else if c.eat("instead") {
        c.expect("of")?;
        "INSTEAD OF"
    } else {
        return Err(syntax(&c.near()));
    };
    let mut events = vec![];
    let mut update_cols = vec![];
    loop {
        let ev = c.ident()?.to_ascii_uppercase();
        if !matches!(ev.as_str(), "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE") {
            return Err(syntax(&ev));
        }
        if ev == "UPDATE" && c.eat("of") {
            loop {
                update_cols.push(c.ident()?);
                if !c.eat_tok(&Token::Comma) {
                    break;
                }
            }
        }
        events.push(ev);
        if !c.eat("or") {
            break;
        }
    }
    c.expect("on")?;
    let table_name = c.qualified()?;
    let table_oid = {
        let tname = table_name.last().cloned().unwrap_or_default();
        ctx.db.find_table_by_name(&tname, &info.search_path).map(|t| t.oid).ok_or_else(|| {
            PgError::new(code::UNDEFINED_TABLE, format!("relation \"{tname}\" does not exist"))
        })?
    };
    let mut row = false;
    let mut when = None;
    loop {
        if c.eat("for") {
            c.eat("each");
            row = c.eat("row");
            if !row {
                c.expect("statement")?;
            }
        } else if c.eat("when") {
            c.expect_tok(Token::LParen)?;
            when = Some(c.until(&[], &[Token::RParen]));
            c.expect_tok(Token::RParen)?;
        } else if c.eat("not")
            || c.eat("deferrable")
            || c.eat("initially")
            || c.eat("deferred")
            || c.eat("immediate")
            || c.eat("referencing")
            || c.eat("from")
        {
            // Constraint-trigger and transition-table clauses: accepted.
            if c.peek().and_then(word_of).is_some_and(|w| w == "new" || w == "old") {
                c.until(&["for", "when", "execute"], &[]);
            }
        } else {
            break;
        }
    }
    c.expect("execute")?;
    if !c.eat("function") {
        c.expect("procedure")?;
    }
    let fname = c.qualified()?.pop().unwrap_or_default();
    c.expect_tok(Token::LParen)?;
    let mut args = vec![];
    while !c.eat_tok(&Token::RParen) {
        match c.peek() {
            Some(t) => {
                args.push(string_of(t).unwrap_or_else(|| t.to_string()));
                c.i += 1;
            }
            None => return Err(syntax("end of input")),
        }
        c.eat_tok(&Token::Comma);
    }
    let func = ctx
        .db
        .functions
        .values()
        .find(|f| f.name == fname && f.arg_types.is_empty())
        .cloned()
        .ok_or_else(|| {
            PgError::new(code::UNDEFINED_FUNCTION, format!("function {fname}() does not exist"))
        })?;
    if func.ret.base != Base::Trigger {
        return Err(PgError::new(
            code::INVALID_OBJECT_DEFINITION,
            format!("function {fname} must return type trigger"),
        ));
    }
    let tname = table_name.last().cloned().unwrap_or_default();
    let existing =
        ctx.db.triggers.values().find(|t| t.table == table_oid && t.name == name).map(|t| t.oid);
    let mut trig = Trigger {
        oid: 0,
        name: name.clone(),
        table: table_oid,
        timing: timing.into(),
        events,
        update_cols,
        row,
        when,
        function: func.oid,
        args,
    };
    match existing {
        Some(oid) if replace => {
            trig.oid = oid;
            ctx.db.triggers.insert(oid, trig);
        }
        Some(_) => {
            return Err(PgError::new(
                code::DUPLICATE_OBJECT,
                format!("trigger \"{name}\" for relation \"{tname}\" already exists"),
            ));
        }
        None => {
            trig.oid = ctx.db.alloc_oid();
            ctx.db.triggers.insert(trig.oid, trig);
        }
    }
    Ok("CREATE TRIGGER".into())
}

fn drop_trigger(ctx: &mut Ctx, info: &SessionInfo, c: &mut Cur) -> PgResult<String> {
    let if_exists = c.eat("if") && {
        c.expect("exists")?;
        true
    };
    let name = c.ident()?;
    c.expect("on")?;
    let tname = c.qualified()?.pop().unwrap_or_default();
    let Some(table) = ctx.db.find_table_by_name(&tname, &info.search_path).map(|t| t.oid) else {
        if if_exists {
            ctx.rt.notices.push(notice(&format!("relation \"{tname}\" does not exist, skipping")));
            return Ok("DROP TRIGGER".into());
        }
        return Err(PgError::new(
            code::UNDEFINED_TABLE,
            format!("relation \"{tname}\" does not exist"),
        ));
    };
    let found =
        ctx.db.triggers.values().find(|t| t.table == table && t.name == name).map(|t| t.oid);
    match found {
        Some(oid) => {
            ctx.db.triggers.remove(&oid);
        }
        None if if_exists => {
            ctx.rt.notices.push(notice(&format!(
                "trigger \"{name}\" for relation \"{tname}\" does not exist, skipping"
            )));
        }
        None => {
            return Err(PgError::new(
                code::UNDEFINED_OBJECT,
                format!("trigger \"{name}\" for table \"{tname}\" does not exist"),
            ));
        }
    }
    Ok("DROP TRIGGER".into())
}

fn notice(msg: &str) -> PgError {
    PgError { severity: "NOTICE", ..PgError::new("00000", msg) }
}

// ---------------------------------------------------------------------------
// PL/pgSQL syntax.

#[derive(Clone, Debug)]
struct Decl {
    name: String,
    ty: String,
    constant: bool,
    not_null: bool,
    default: Option<String>,
    /// `name ALIAS FOR $1`.
    alias: Option<String>,
}

#[derive(Clone, Debug)]
struct Block {
    label: Option<String>,
    decls: Vec<Decl>,
    body: Vec<Stmt>,
    handlers: Vec<(Vec<String>, Vec<Stmt>)>,
}

#[derive(Clone, Debug)]
enum Stmt {
    Block(Block),
    Assign {
        target: String,
        expr: String,
    },
    If {
        arms: Vec<(String, Vec<Stmt>)>,
        els: Option<Vec<Stmt>>,
    },
    Case {
        subject: Option<String>,
        arms: Vec<(String, Vec<Stmt>)>,
        els: Option<Vec<Stmt>>,
    },
    Loop {
        label: Option<String>,
        body: Vec<Stmt>,
    },
    While {
        label: Option<String>,
        cond: String,
        body: Vec<Stmt>,
    },
    ForInt {
        label: Option<String>,
        var: String,
        reverse: bool,
        lo: String,
        hi: String,
        by: Option<String>,
        body: Vec<Stmt>,
    },
    ForQuery {
        label: Option<String>,
        var: String,
        query: String,
        dynamic: bool,
        body: Vec<Stmt>,
    },
    ForEach {
        label: Option<String>,
        var: String,
        array: String,
        body: Vec<Stmt>,
    },
    Exit {
        cont: bool,
        label: Option<String>,
        when: Option<String>,
    },
    Return(Option<String>),
    ReturnNext(Option<String>),
    ReturnQuery(String),
    Raise {
        level: String,
        fmt: Option<String>,
        args: Vec<String>,
        using: Vec<(String, String)>,
        condition: Option<String>,
    },
    Perform(String),
    Sql {
        sql: String,
        into: Vec<String>,
        strict: bool,
    },
    Execute {
        sql: String,
        into: Vec<String>,
        strict: bool,
        using: Vec<String>,
    },
    GetDiag(Vec<(String, String)>),
    Assert {
        cond: String,
        msg: Option<String>,
    },
    Null,
}

struct Parser<'s> {
    c: Cur<'s>,
}

impl Parser<'_> {
    fn parse_body(body: &str) -> PgResult<Block> {
        let mut p = Parser { c: Cur::new(body)? };
        let b = p.block(None)?;
        p.c.eat_tok(&Token::SemiColon);
        if !p.c.at_end() {
            return Err(syntax(&p.c.near()));
        }
        Ok(b)
    }

    /// `<<label>>` if present.
    fn label(&mut self) -> PgResult<Option<String>> {
        if self.c.peek() == Some(&Token::ShiftLeft) {
            self.c.i += 1;
            let l = self.c.ident()?;
            self.c.expect_tok(Token::ShiftRight)?;
            return Ok(Some(l));
        }
        Ok(None)
    }

    fn block(&mut self, label: Option<String>) -> PgResult<Block> {
        let label = match label {
            Some(l) => Some(l),
            None => self.label()?,
        };
        let mut decls = vec![];
        if self.c.eat("declare") {
            while self.c.word().as_deref() != Some("begin") {
                if self.c.at_end() {
                    return Err(syntax("end of input"));
                }
                decls.push(self.decl()?);
            }
        }
        self.c.expect("begin")?;
        let body = self.stmts(&["end", "exception"])?;
        let mut handlers = vec![];
        if self.c.eat("exception") {
            while self.c.eat("when") {
                let mut conds = vec![];
                loop {
                    if self.c.eat("sqlstate") {
                        let s = self
                            .c
                            .peek()
                            .and_then(string_of)
                            .ok_or_else(|| syntax(&self.c.near()))?;
                        self.c.i += 1;
                        conds.push(format!("sqlstate:{s}"));
                    } else {
                        conds.push(self.c.ident()?);
                    }
                    if !self.c.eat("or") {
                        break;
                    }
                }
                self.c.expect("then")?;
                let stmts = self.stmts(&["when", "end"])?;
                handlers.push((conds, stmts));
            }
        }
        self.c.expect("end")?;
        if self.c.peek().and_then(word_of).is_some_and(|w| Some(&w) == label.as_ref()) {
            self.c.i += 1;
        }
        Ok(Block { label, decls, body, handlers })
    }

    fn decl(&mut self) -> PgResult<Decl> {
        let name = self.c.ident()?;
        if self.c.eat("alias") {
            self.c.expect("for")?;
            let target = self.c.until(&[], &[]);
            self.c.expect_tok(Token::SemiColon)?;
            return Ok(Decl {
                name,
                ty: String::new(),
                constant: false,
                not_null: false,
                default: None,
                alias: Some(target),
            });
        }
        let constant = self.c.eat("constant");
        let ty = self.c.until(&["default", "not", "collate"], &[Token::Assignment, Token::Eq]);
        let not_null = self.c.eat("not") && {
            self.c.expect("null")?;
            true
        };
        let default = if self.c.eat("default")
            || self.c.eat_tok(&Token::Assignment)
            || self.c.eat_tok(&Token::Eq)
        {
            Some(self.c.until(&[], &[]))
        } else {
            None
        };
        self.c.expect_tok(Token::SemiColon)?;
        Ok(Decl { name, ty, constant, not_null, default, alias: None })
    }

    fn stmts(&mut self, stops: &[&str]) -> PgResult<Vec<Stmt>> {
        let mut out = vec![];
        loop {
            if self.c.at_end() {
                return Err(syntax("end of input"));
            }
            if self.c.word().is_some_and(|w| stops.contains(&w.as_str())) {
                // `END IF`/`END LOOP` etc. close the caller's construct.
                return Ok(out);
            }
            out.push(self.stmt()?);
        }
    }

    /// `END IF;` / `END CASE;`
    fn end(&mut self, what: &str) -> PgResult<()> {
        self.c.expect("end")?;
        self.c.expect(what)?;
        self.c.expect_tok(Token::SemiColon)
    }

    fn stmt(&mut self) -> PgResult<Stmt> {
        let label = self.label()?;
        let w = self.c.word().unwrap_or_default();
        let s = match w.as_str() {
            "declare" | "begin" => Stmt::Block(self.block(label)?),
            "if" => {
                self.c.i += 1;
                let mut arms = vec![];
                let cond = self.c.until(&["then"], &[]);
                self.c.expect("then")?;
                arms.push((cond, self.stmts(&["elsif", "elseif", "else", "end"])?));
                let mut els = None;
                loop {
                    if self.c.eat("elsif") || self.c.eat("elseif") {
                        let cond = self.c.until(&["then"], &[]);
                        self.c.expect("then")?;
                        arms.push((cond, self.stmts(&["elsif", "elseif", "else", "end"])?));
                    } else if self.c.eat("else") {
                        els = Some(self.stmts(&["end"])?);
                    } else {
                        break;
                    }
                }
                self.end("if")?;
                Stmt::If { arms, els }
            }
            "case" => {
                self.c.i += 1;
                let subject = if self.c.word().as_deref() == Some("when") {
                    None
                } else {
                    Some(self.c.until(&["when"], &[]))
                };
                let mut arms = vec![];
                let mut els = None;
                loop {
                    if self.c.eat("when") {
                        let cond = self.c.until(&["then"], &[]);
                        self.c.expect("then")?;
                        arms.push((cond, self.stmts(&["when", "else", "end"])?));
                    } else if self.c.eat("else") {
                        els = Some(self.stmts(&["end"])?);
                    } else {
                        break;
                    }
                }
                self.end("case")?;
                Stmt::Case { subject, arms, els }
            }
            "loop" => {
                self.c.i += 1;
                let body = self.stmts(&["end"])?;
                self.end_loop(&label)?;
                Stmt::Loop { label, body }
            }
            "while" => {
                self.c.i += 1;
                let cond = self.c.until(&["loop"], &[]);
                self.c.expect("loop")?;
                let body = self.stmts(&["end"])?;
                self.end_loop(&label)?;
                Stmt::While { label, cond, body }
            }
            "for" => {
                self.c.i += 1;
                let var = self.c.ident()?;
                self.c.expect("in")?;
                let reverse = self.c.eat("reverse");
                // An integer range has a top-level `..`.
                let save = self.c.i;
                let first = self.c.until(&["loop"], &[]);
                let is_range = {
                    let probe = Cur::new(&first)?;
                    let mut depth = 0;
                    probe.toks.iter().any(|t| match &t.t {
                        Token::LParen => {
                            depth += 1;
                            false
                        }
                        Token::RParen => {
                            depth -= 1;
                            false
                        }
                        Token::Period => depth == 0,
                        _ => false,
                    }) && first.contains("..")
                };
                if is_range {
                    let (lo, rest) = first.split_once("..").ok_or_else(|| syntax(&first))?;
                    let (hi, by) = match split_word(rest, "by") {
                        Some((h, b)) => (h, Some(b)),
                        None => (rest.to_string(), None),
                    };
                    self.c.expect("loop")?;
                    let body = self.stmts(&["end"])?;
                    self.end_loop(&label)?;
                    Stmt::ForInt {
                        label,
                        var,
                        reverse,
                        lo: lo.trim().into(),
                        hi: hi.trim().into(),
                        by: by.map(|b| b.trim().to_string()),
                        body,
                    }
                } else {
                    self.c.i = save;
                    let dynamic = self.c.eat("execute");
                    let query = self.c.until(&["loop"], &[]);
                    self.c.expect("loop")?;
                    let body = self.stmts(&["end"])?;
                    self.end_loop(&label)?;
                    Stmt::ForQuery { label, var, query, dynamic, body }
                }
            }
            "foreach" => {
                self.c.i += 1;
                let var = self.c.ident()?;
                self.c.expect("in")?;
                self.c.expect("array")?;
                let array = self.c.until(&["loop"], &[]);
                self.c.expect("loop")?;
                let body = self.stmts(&["end"])?;
                self.end_loop(&label)?;
                Stmt::ForEach { label, var, array, body }
            }
            "exit" | "continue" => {
                self.c.i += 1;
                let lbl = if self.c.word().is_some_and(|w| w != "when")
                    && self.c.peek() != Some(&Token::SemiColon)
                {
                    Some(self.c.ident()?)
                } else {
                    None
                };
                let when = if self.c.eat("when") { Some(self.c.until(&[], &[])) } else { None };
                self.c.expect_tok(Token::SemiColon)?;
                return Ok(Stmt::Exit { cont: w == "continue", label: lbl, when });
            }
            "return" => {
                self.c.i += 1;
                let s = if self.c.eat("next") {
                    let e = self.c.until(&[], &[]);
                    Stmt::ReturnNext((!e.is_empty()).then_some(e))
                } else if self.c.eat("query") {
                    let dynamic = self.c.eat("execute");
                    let q = self.c.until(&[], &[]);
                    if dynamic {
                        return Err(PgError::new(
                            code::FEATURE_NOT_SUPPORTED,
                            "RETURN QUERY EXECUTE is not supported",
                        ));
                    }
                    Stmt::ReturnQuery(q)
                } else {
                    let e = self.c.until(&[], &[]);
                    Stmt::Return((!e.is_empty()).then_some(e))
                };
                self.c.expect_tok(Token::SemiColon)?;
                s
            }
            "raise" => {
                self.c.i += 1;
                let level = match self.c.word().as_deref() {
                    Some(l @ ("debug" | "log" | "info" | "notice" | "warning" | "exception")) => {
                        let l = l.to_string();
                        self.c.i += 1;
                        l
                    }
                    _ => "exception".into(),
                };
                let mut fmt = None;
                let mut args = vec![];
                let mut condition = None;
                if let Some(s) = self.c.peek().and_then(string_of) {
                    fmt = Some(s);
                    self.c.i += 1;
                    while self.c.eat_tok(&Token::Comma) {
                        args.push(self.c.until(&["using"], &[Token::Comma]));
                    }
                } else if self.c.eat("sqlstate") {
                    let s =
                        self.c.peek().and_then(string_of).ok_or_else(|| syntax(&self.c.near()))?;
                    self.c.i += 1;
                    condition = Some(format!("sqlstate:{s}"));
                } else if self.c.word().is_some_and(|w| w != "using")
                    && self.c.peek() != Some(&Token::SemiColon)
                {
                    condition = Some(self.c.ident()?);
                }
                let mut using = vec![];
                if self.c.eat("using") {
                    loop {
                        let opt = self.c.ident()?;
                        if !self.c.eat_tok(&Token::Eq) {
                            self.c.expect_tok(Token::Assignment)?;
                        }
                        using.push((opt, self.c.until(&[], &[Token::Comma])));
                        if !self.c.eat_tok(&Token::Comma) {
                            break;
                        }
                    }
                }
                self.c.expect_tok(Token::SemiColon)?;
                Stmt::Raise { level, fmt, args, using, condition }
            }
            "perform" => {
                self.c.i += 1;
                let q = self.c.until(&[], &[]);
                self.c.expect_tok(Token::SemiColon)?;
                Stmt::Perform(q)
            }
            "null" if self.c.peek_at(1) == Some(&Token::SemiColon) => {
                self.c.i += 2;
                Stmt::Null
            }
            "assert" => {
                self.c.i += 1;
                let cond = self.c.until(&[], &[Token::Comma]);
                let msg =
                    if self.c.eat_tok(&Token::Comma) { Some(self.c.until(&[], &[])) } else { None };
                self.c.expect_tok(Token::SemiColon)?;
                Stmt::Assert { cond, msg }
            }
            "get" => {
                self.c.i += 1;
                self.c.eat("current");
                self.c.expect("diagnostics")?;
                let mut items = vec![];
                loop {
                    let var = self.c.ident()?;
                    if !self.c.eat_tok(&Token::Eq) {
                        self.c.expect_tok(Token::Assignment)?;
                    }
                    items.push((var, self.c.ident()?));
                    if !self.c.eat_tok(&Token::Comma) {
                        break;
                    }
                }
                self.c.expect_tok(Token::SemiColon)?;
                Stmt::GetDiag(items)
            }
            "execute" => {
                self.c.i += 1;
                let sql = self.c.until(&["into", "using"], &[]);
                let (mut into, mut strict, mut using) = (vec![], false, vec![]);
                loop {
                    if self.c.eat("into") {
                        strict = self.c.eat("strict");
                        loop {
                            into.push(self.c.until(&["using"], &[Token::Comma]));
                            if !self.c.eat_tok(&Token::Comma) {
                                break;
                            }
                        }
                    } else if self.c.eat("using") {
                        loop {
                            using.push(self.c.until(&["into"], &[Token::Comma]));
                            if !self.c.eat_tok(&Token::Comma) {
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
                self.c.expect_tok(Token::SemiColon)?;
                Stmt::Execute { sql, into, strict, using }
            }
            "commit" | "rollback" => {
                return Err(PgError::new(
                    code::FEATURE_NOT_SUPPORTED,
                    "COMMIT and ROLLBACK inside a procedure are not supported",
                ));
            }
            _ => {
                // An assignment (`x := e`, `rec.f = e`) or a SQL statement.
                let start = self.c.i;
                let mut k = 0;
                while matches!(self.c.peek_at(k), Some(Token::Word(_)))
                    && matches!(self.c.peek_at(k + 1), Some(Token::Period))
                {
                    k += 2;
                }
                let is_assign = matches!(self.c.peek_at(k), Some(Token::Word(_)))
                    && matches!(self.c.peek_at(k + 1), Some(Token::Assignment | Token::Eq))
                    && !matches!(w.as_str(), "select" | "insert" | "update" | "delete" | "with");
                if is_assign {
                    let target = self.c.text(start, start + k + 1).to_ascii_lowercase();
                    self.c.i = start + k + 2;
                    let expr = self.c.until(&[], &[]);
                    self.c.expect_tok(Token::SemiColon)?;
                    return Ok(Stmt::Assign { target, expr });
                }
                let sql = self.c.until(&[], &[]);
                self.c.expect_tok(Token::SemiColon)?;
                let (sql, into, strict) = split_into(&sql)?;
                Stmt::Sql { sql, into, strict }
            }
        };
        if !matches!(s, Stmt::Block(_)) {
            return Ok(s);
        }
        self.c.eat_tok(&Token::SemiColon);
        Ok(s)
    }

    fn end_loop(&mut self, label: &Option<String>) -> PgResult<()> {
        self.c.expect("end")?;
        self.c.expect("loop")?;
        if self.c.peek().and_then(word_of).is_some_and(|w| Some(&w) == label.as_ref()) {
            self.c.i += 1;
        }
        self.c.expect_tok(Token::SemiColon)
    }
}

/// Splits `text` at the first top-level word `w` (outside parentheses).
fn split_word(text: &str, w: &str) -> Option<(String, String)> {
    let c = Cur::new(text).ok()?;
    let mut depth = 0;
    for t in &c.toks {
        match &t.t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            other if depth == 0 && word_of(other).as_deref() == Some(w) => {
                return Some((text[..t.start].to_string(), text[t.end..].to_string()));
            }
            _ => {}
        }
    }
    None
}

/// `SELECT a, b INTO [STRICT] x, y FROM ...` / `... RETURNING id INTO x`:
/// the statement without its INTO clause, and the targets.
fn split_into(sql: &str) -> PgResult<(String, Vec<String>, bool)> {
    let c = Cur::new(sql)?;
    let first = c.toks.first().and_then(|t| word_of(&t.t)).unwrap_or_default();
    let mut depth = 0;
    let mut seen_returning = false;
    for (i, t) in c.toks.iter().enumerate() {
        match &t.t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            other if depth == 0 => {
                let w = word_of(other).unwrap_or_default();
                if w == "returning" {
                    seen_returning = true;
                }
                let applies = match first.as_str() {
                    "select" | "with" => w == "into",
                    "insert" | "update" | "delete" => w == "into" && seen_returning,
                    _ => false,
                };
                if !applies {
                    continue;
                }
                // Targets: identifiers (or rec.field) separated by commas.
                let mut j = i + 1;
                let strict = c.toks.get(j).and_then(|t| word_of(&t.t)).as_deref() == Some("strict");
                if strict {
                    j += 1;
                }
                let mut targets = vec![];
                loop {
                    let start = j;
                    while matches!(c.toks.get(j).map(|t| &t.t), Some(Token::Word(_)))
                        && matches!(c.toks.get(j + 1).map(|t| &t.t), Some(Token::Period))
                    {
                        j += 2;
                    }
                    if !matches!(c.toks.get(j).map(|t| &t.t), Some(Token::Word(_))) {
                        return Err(syntax(&sql[t.start..]));
                    }
                    j += 1;
                    targets.push(c.text(start, j).to_ascii_lowercase());
                    if matches!(c.toks.get(j).map(|t| &t.t), Some(Token::Comma)) {
                        j += 1;
                    } else {
                        break;
                    }
                }
                let end = c.toks.get(j).map_or(sql.len(), |t| t.start);
                let stripped = format!("{} {}", &sql[..t.start], &sql[end..]);
                return Ok((stripped.trim().to_string(), targets, strict));
            }
            _ => {}
        }
    }
    Ok((sql.to_string(), vec![], false))
}

// ---------------------------------------------------------------------------
// Execution.

#[derive(Clone, Debug)]
enum Var {
    Scalar {
        v: Value,
        ty: Type,
        constant: bool,
    },
    /// A row: NEW/OLD, a query loop's record, `%ROWTYPE`.
    Rec(Option<Rec>),
    /// NEW in a DELETE trigger, OLD in an INSERT one: NULL, but its fields
    /// still have their columns' types (`OLD.id` is a NULL int).
    NullRow(Rec),
}

#[derive(Clone, Debug)]
struct Rec {
    cols: Vec<String>,
    tys: Vec<Type>,
    vals: Vec<Value>,
}

enum Flow {
    Normal,
    Exit(Option<String>),
    Continue(Option<String>),
    Return,
}

struct Frame<'f> {
    func: &'f Function,
    scopes: Vec<HashMap<String, Var>>,
    /// The value RETURN gave (scalar functions), or the row (triggers).
    ret: Option<Value>,
    ret_row: Option<Option<Rec>>,
    /// RETURN NEXT / RETURN QUERY rows (set-returning functions).
    set_rows: Vec<Row>,
    row_count: i64,
    /// The error an EXCEPTION handler is handling (for `RAISE;`).
    handling: Option<PgError>,
}

impl<'f> Frame<'f> {
    fn new(func: &'f Function) -> Self {
        let mut top = HashMap::new();
        top.insert(
            "found".into(),
            Var::Scalar { v: Value::Bool(false), ty: Type::BOOL, constant: false },
        );
        Frame {
            func,
            scopes: vec![top],
            ret: None,
            ret_row: None,
            set_rows: vec![],
            row_count: 0,
            handling: None,
        }
    }
    fn get(&self, name: &str) -> Option<&Var> {
        self.scopes.iter().rev().find_map(|s| s.get(name))
    }
    fn get_mut(&mut self, name: &str) -> Option<&mut Var> {
        self.scopes.iter_mut().rev().find_map(|s| s.get_mut(name))
    }
    fn set_found(&mut self, f: bool) {
        if let Some(Var::Scalar { v, .. }) = self.get_mut("found") {
            *v = Value::Bool(f);
        }
    }
}

fn enter() -> PgResult<()> {
    let d = DEPTH.with(|d| {
        d.set(d.get() + 1);
        d.get()
    });
    if d > MAX_DEPTH {
        DEPTH.with(|d| d.set(d.get() - 1));
        return Err(PgError::new("54001", "stack depth limit exceeded"));
    }
    Ok(())
}

fn leave() {
    DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
}

/// Runs `sql` (one statement) in the current transaction.
fn run_sql(
    ctx: &mut Ctx,
    info: &SessionInfo,
    sql: &str,
    params: &[Value],
    types: &[Type],
) -> PgResult<super::engine::StmtResult> {
    let mut stmts = super::parse_sql(sql)?;
    if stmts.len() != 1 {
        return Err(PgError::new(
            code::SYNTAX_ERROR,
            format!("expected one statement in \"{sql}\""),
        ));
    }
    let stmt = stmts.remove(0);
    let databases = ctx.databases.clone();
    let mut nested = Ctx {
        db: &mut *ctx.db,
        seqs: &mut *ctx.seqs,
        rt: &mut *ctx.rt,
        params,
        outer: vec![],
        ctes: vec![],
        notifies: vec![],
        affected: 0,
        databases,
        subq_cache: vec![],
        min_outer: usize::MAX,
    };
    let r = super::engine::run_one(&mut nested, &stmt, info, types);
    let notifies = std::mem::take(&mut nested.notifies);
    drop(nested);
    ctx.notifies.extend(notifies);
    r
}

/// `text` with every variable reference replaced by a `$n` parameter.
fn substitute(frame: &Frame, text: &str) -> PgResult<(String, Vec<Value>, Vec<Type>)> {
    substitute_seeded(frame, text, &[])
}

/// `substitute`, with `seed` as parameters `$1..$k` already (a SQL
/// function's arguments, which its body may name as `$1`), so variable
/// references are numbered after them.
fn substitute_seeded(
    frame: &Frame,
    text: &str,
    seed: &[(Value, Type)],
) -> PgResult<(String, Vec<Value>, Vec<Type>)> {
    let c = Cur::new(text)?;
    let mut out = String::new();
    let mut last = 0usize;
    let mut params: Vec<Value> = seed.iter().map(|(v, _)| v.clone()).collect();
    let mut types: Vec<Type> = seed.iter().map(|(_, t)| *t).collect();
    let mut keys: Vec<String> = (1..=seed.len()).map(|i| format!("${i}")).collect();
    let first = c.toks.first().and_then(|t| word_of(&t.t)).unwrap_or_default();
    let mut add = |key: String, v: Value, ty: Type| -> usize {
        if let Some(i) = keys.iter().position(|k| *k == key) {
            return i + 1;
        }
        keys.push(key);
        params.push(v);
        types.push(ty);
        params.len()
    };
    // Column lists that name columns, not variables: INSERT INTO t (...),
    // ON CONFLICT (...), and the targets of UPDATE ... SET.
    let mut skip_paren_depth: Option<i32> = None;
    let mut depth = 0i32;
    let mut i = 0;
    while i < c.toks.len() {
        let t = &c.toks[i];
        let prev = i.checked_sub(1).map(|p| &c.toks[p].t);
        let prev_word = prev.and_then(word_of);
        match &t.t {
            Token::LParen => {
                depth += 1;
                let opener = i.checked_sub(1).and_then(|p| c.toks.get(p));
                let before = i.checked_sub(2).and_then(|p| c.toks.get(p));
                let names_cols = (first == "insert"
                    && before.and_then(|b| word_of(&b.t)).as_deref() == Some("into")
                    && matches!(opener.map(|o| &o.t), Some(Token::Word(_))))
                    || prev_word.as_deref() == Some("conflict");
                if names_cols && skip_paren_depth.is_none() {
                    skip_paren_depth = Some(depth);
                }
            }
            Token::RParen => {
                if skip_paren_depth == Some(depth) {
                    skip_paren_depth = None;
                }
                depth -= 1;
            }
            Token::Word(_) if skip_paren_depth.is_none() => {
                let name = ident_of(&t.t).unwrap_or_default();
                let next = c.toks.get(i + 1).map(|n| &n.t);
                let after_dot = matches!(prev, Some(Token::Period));
                let is_call = matches!(next, Some(Token::LParen));
                let is_alias = prev_word.as_deref() == Some("as");
                let set_target = first == "update"
                    && matches!(next, Some(Token::Eq))
                    && (prev_word.as_deref() == Some("set") || matches!(prev, Some(Token::Comma)));
                if !after_dot && !is_call && !is_alias && !set_target {
                    match frame.get(&name) {
                        Some(Var::Scalar { v, ty, .. }) => {
                            let n = add(name.clone(), v.clone(), *ty);
                            out.push_str(&text[last..t.start]);
                            out.push_str(&format!("${n}"));
                            last = t.end;
                        }
                        Some(Var::NullRow(shape)) => {
                            if matches!(next, Some(Token::Period))
                                && let Some(ft) = c.toks.get(i + 2)
                                && let Some(field) = ident_of(&ft.t)
                                && let Some(k) = shape.cols.iter().position(|c| *c == field)
                            {
                                let n = add(format!("{name}.{field}"), Value::Null, shape.tys[k]);
                                out.push_str(&text[last..t.start]);
                                out.push_str(&format!("${n}"));
                                last = ft.end;
                                i += 3;
                                continue;
                            }
                        }
                        Some(Var::Rec(rec)) => {
                            // `rec.field`
                            if matches!(next, Some(Token::Period))
                                && let Some(ft) = c.toks.get(i + 2)
                                && let Some(field) = ident_of(&ft.t)
                            {
                                let Some(r) = rec else {
                                    return Err(PgError::new(
                                        "55000",
                                        format!("record \"{name}\" is not assigned yet"),
                                    ));
                                };
                                let Some(k) = r.cols.iter().position(|c| *c == field) else {
                                    return Err(PgError::new(
                                        code::UNDEFINED_COLUMN,
                                        format!("record \"{name}\" has no field \"{field}\""),
                                    ));
                                };
                                let n = add(format!("{name}.{field}"), r.vals[k].clone(), r.tys[k]);
                                out.push_str(&text[last..t.start]);
                                out.push_str(&format!("${n}"));
                                last = ft.end;
                                i += 3;
                                continue;
                            }
                        }
                        None => {}
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    out.push_str(&text[last..]);
    Ok((out, params, types))
}

/// Evaluates a PL/pgSQL expression (cast to `ty` when given).
fn eval_expr(
    ctx: &mut Ctx,
    info: &SessionInfo,
    frame: &Frame,
    expr: &str,
    ty: Option<Type>,
) -> PgResult<Value> {
    let (sql, params, types) = substitute(frame, expr)?;
    let sql = match ty {
        Some(t) if !matches!(t.base, Base::Record | Base::Unknown | Base::Void | Base::Trigger) => {
            format!("SELECT ({sql})::{}", type_sql(t))
        }
        _ => format!("SELECT {sql}"),
    };
    let r = run_sql(ctx, info, &sql, &params, &types)?;
    if r.rows.len() > 1 {
        return Err(PgError::new(code::CARDINALITY_VIOLATION, "query returned more than one row"));
    }
    Ok(r.rows.into_iter().next().and_then(|r| r.into_iter().next()).unwrap_or(Value::Null))
}

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

/// Calls a user function or procedure with `args` (already its argument
/// types): its result, or for a set-returning function its rows.
pub fn call_function(
    ctx: &mut Ctx,
    info: &SessionInfo,
    f: &Function,
    mut args: Vec<Value>,
) -> PgResult<Value> {
    // Missing trailing arguments take their defaults.
    while args.len() < f.arg_types.len() {
        let i = args.len();
        let d = f.arg_defaults[i].clone().unwrap_or_else(|| "NULL".into());
        let v =
            run_sql(ctx, info, &format!("SELECT ({d})::{}", type_sql(f.arg_types[i])), &[], &[])?
                .rows
                .into_iter()
                .next()
                .and_then(|r| r.into_iter().next())
                .unwrap_or(Value::Null);
        args.push(v);
    }
    if f.strict && args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    enter()?;
    let r = call_inner(ctx, info, f, args);
    leave();
    r
}

fn call_inner(
    ctx: &mut Ctx,
    info: &SessionInfo,
    f: &Function,
    args: Vec<Value>,
) -> PgResult<Value> {
    if f.language == "sql" {
        // `$n` and argument names refer to the arguments; the result is
        // the last statement's first column of its first row.
        let mut frame = Frame::new(f);
        for (i, (n, v)) in f.arg_names.iter().zip(&args).enumerate() {
            let var = Var::Scalar { v: v.clone(), ty: f.arg_types[i], constant: true };
            if !n.is_empty() {
                frame.scopes[0].insert(n.clone(), var.clone());
            }
            frame.scopes[0].insert(format!("${}", i + 1), var);
        }
        let stmts: Vec<String> = split_statements(&f.body)?;
        let seed: Vec<(Value, Type)> =
            args.iter().cloned().zip(f.arg_types.iter().copied()).collect();
        let mut last = None;
        for s in &stmts {
            let (sql, params, types) = substitute_seeded(&frame, s, &seed)?;
            last = Some(run_sql(ctx, info, &sql, &params, &types)?);
        }
        let rows = last.map(|r| r.rows).unwrap_or_default();
        if f.returns_set {
            return Ok(set_value(rows));
        }
        return Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .unwrap_or(Value::Null));
    }
    let block = Parser::parse_body(&f.body)?;
    let mut frame = Frame::new(f);
    for (i, v) in args.iter().enumerate() {
        let var = Var::Scalar { v: v.clone(), ty: f.arg_types[i], constant: false };
        if !f.arg_names[i].is_empty() {
            frame.scopes[0].insert(f.arg_names[i].clone(), var.clone());
        }
        frame.scopes[0].insert(format!("${}", i + 1), var);
    }
    let flow = exec_block(ctx, info, &mut frame, &block)?;
    if f.returns_set {
        return Ok(set_value(std::mem::take(&mut frame.set_rows)));
    }
    if f.procedure || f.ret.base == Base::Void {
        return Ok(Value::Null);
    }
    match (flow, frame.ret) {
        (Flow::Return, Some(v)) => Ok(v),
        (Flow::Return, None) => Ok(Value::Null),
        _ => Err(PgError::new("2F005", "control reached end of function without RETURN")),
    }
}

/// A set-returning function's rows travel as an array of records.
fn set_value(rows: Vec<Row>) -> Value {
    Value::Array(Box::new(types::Array::new(rows.into_iter().map(Value::Record).collect())))
}

/// The rows a set-returning user function produced (see `set_value`).
pub fn set_rows(v: Value) -> Vec<Row> {
    match v {
        Value::Array(a) => a
            .items
            .into_iter()
            .map(|r| match r {
                Value::Record(r) => r,
                other => vec![other],
            })
            .collect(),
        Value::Null => vec![],
        other => vec![vec![other]],
    }
}

fn split_statements(body: &str) -> PgResult<Vec<String>> {
    let c = Cur::new(body)?;
    let mut out = vec![];
    let mut start = 0;
    for t in &c.toks {
        if t.t == Token::SemiColon {
            let s = body[start..t.start].trim();
            if !s.is_empty() {
                out.push(s.to_string());
            }
            start = t.end;
        }
    }
    let s = body[start..].trim();
    if !s.is_empty() {
        out.push(s.to_string());
    }
    Ok(out)
}

fn exec_stmts(
    ctx: &mut Ctx,
    info: &SessionInfo,
    frame: &mut Frame,
    stmts: &[Stmt],
) -> PgResult<Flow> {
    for s in stmts {
        match exec_stmt(ctx, info, frame, s)? {
            Flow::Normal => {}
            other => return Ok(other),
        }
    }
    Ok(Flow::Normal)
}

fn exec_block(ctx: &mut Ctx, info: &SessionInfo, frame: &mut Frame, b: &Block) -> PgResult<Flow> {
    frame.scopes.push(HashMap::new());
    let r = exec_block_inner(ctx, info, frame, b);
    frame.scopes.pop();
    match r {
        Ok(Flow::Exit(Some(l))) if Some(&l) == b.label.as_ref() => Ok(Flow::Normal),
        other => other,
    }
}

fn exec_block_inner(
    ctx: &mut Ctx,
    info: &SessionInfo,
    frame: &mut Frame,
    b: &Block,
) -> PgResult<Flow> {
    for d in &b.decls {
        if let Some(target) = &d.alias {
            let v = frame.get(target.trim()).cloned().ok_or_else(|| syntax(target))?;
            frame.scopes.last_mut().unwrap().insert(d.name.clone(), v);
            continue;
        }
        let ty = parse_type(&d.ty, ctx.db, info)?;
        let var = if ty.base == Base::Record {
            Var::Rec(None)
        } else {
            let v = match &d.default {
                Some(e) => eval_expr(ctx, info, frame, e, Some(ty))?,
                None => Value::Null,
            };
            if d.not_null && v.is_null() {
                return Err(PgError::new(
                    code::NULL_VALUE_NOT_ALLOWED,
                    format!(
                        "variable \"{}\" must have a default value, since it's declared NOT NULL",
                        d.name
                    ),
                ));
            }
            Var::Scalar { v, ty, constant: d.constant }
        };
        frame.scopes.last_mut().unwrap().insert(d.name.clone(), var);
    }
    if b.handlers.is_empty() {
        return exec_stmts(ctx, info, frame, &b.body);
    }
    // A block with handlers is a subtransaction: an error rolls back what
    // the block did before the handler runs.
    let saved = ctx.db.clone();
    match exec_stmts(ctx, info, frame, &b.body) {
        Ok(f) => Ok(f),
        Err(e) => {
            let Some((_, stmts)) = b
                .handlers
                .iter()
                .find(|(conds, _)| conds.iter().any(|c| matches_condition(c, e.code)))
            else {
                return Err(e);
            };
            *ctx.db = saved;
            frame.scopes.push(HashMap::new());
            let scope = frame.scopes.last_mut().unwrap();
            scope.insert(
                "sqlstate".into(),
                Var::Scalar { v: Value::text(e.code.to_string()), ty: Type::TEXT, constant: true },
            );
            scope.insert(
                "sqlerrm".into(),
                Var::Scalar { v: Value::text(e.message.clone()), ty: Type::TEXT, constant: true },
            );
            let prev = frame.handling.replace(e);
            let r = exec_stmts(ctx, info, frame, stmts);
            frame.handling = prev;
            frame.scopes.pop();
            r
        }
    }
}

/// Whether an EXCEPTION condition catches SQLSTATE `state`.
fn matches_condition(cond: &str, state: &str) -> bool {
    if let Some(s) = cond.strip_prefix("sqlstate:") {
        return s == state;
    }
    let code = match cond {
        "others" => return state != "57014",
        "unique_violation" => "23505",
        "foreign_key_violation" => "23503",
        "not_null_violation" => "23502",
        "check_violation" => "23514",
        "exclusion_violation" => "23P01",
        "restrict_violation" => "23001",
        "integrity_constraint_violation" => return state.starts_with("23"),
        "division_by_zero" => "22012",
        "numeric_value_out_of_range" => "22003",
        "invalid_text_representation" => "22P02",
        "string_data_right_truncation" => "22001",
        "invalid_datetime_format" => "22007",
        "datetime_field_overflow" => "22008",
        "null_value_not_allowed" => "22004",
        "data_exception" => return state.starts_with("22"),
        "raise_exception" => "P0001",
        "no_data_found" => "P0002",
        "too_many_rows" => "P0003",
        "assert_failure" => "P0004",
        "undefined_table" => "42P01",
        "undefined_column" => "42703",
        "undefined_function" => "42883",
        "undefined_object" => "42704",
        "duplicate_table" => "42P07",
        "duplicate_object" => "42710",
        "syntax_error" => "42601",
        "insufficient_privilege" => "42501",
        "lock_not_available" => "55P03",
        "deadlock_detected" => "40P01",
        "serialization_failure" => "40001",
        "invalid_parameter_value" => "22023",
        "feature_not_supported" => "0A000",
        _ => return false,
    };
    code == state
}

/// The SQLSTATE for a condition name (RAISE condition / ERRCODE).
fn condition_code(cond: &str) -> String {
    if let Some(s) = cond.strip_prefix("sqlstate:") {
        return s.to_string();
    }
    if cond.len() == 5
        && cond.chars().all(|c| c.is_ascii_alphanumeric())
        && cond.chars().any(|c| c.is_ascii_digit())
    {
        return cond.to_ascii_uppercase();
    }
    for probe in [
        "23505", "23503", "23502", "23514", "22012", "22003", "22P02", "P0001", "P0002", "P0003",
        "P0004", "42P01", "42703", "42883", "42704", "42601", "22023", "0A000", "40001", "40P01",
    ] {
        if matches_condition(cond, probe) {
            return probe.to_string();
        }
    }
    "P0001".into()
}

fn assign(
    ctx: &mut Ctx,
    info: &SessionInfo,
    frame: &mut Frame,
    target: &str,
    v: Value,
    given_ty: Option<Type>,
) -> PgResult<()> {
    let (name, field) = match target.split_once('.') {
        Some((n, f)) => (n.to_string(), Some(f.to_string())),
        None => (target.to_string(), None),
    };
    let coerce = |ctx: &mut Ctx, v: Value, from: Option<Type>, to: Type| -> PgResult<Value> {
        if v.is_null() || from == Some(to) || matches!(to.base, Base::Record | Base::Unknown) {
            return Ok(v);
        }
        let from = from.unwrap_or(Type::TEXT);
        let r = run_sql(
            ctx,
            info,
            &format!("SELECT ($1::{})::{}", type_sql(from), type_sql(to)),
            &[v],
            &[from],
        )?;
        Ok(r.rows.into_iter().next().and_then(|r| r.into_iter().next()).unwrap_or(Value::Null))
    };
    match (frame.get(&name).cloned(), field) {
        (Some(Var::Scalar { ty, constant, .. }), None) => {
            if constant {
                return Err(PgError::new(
                    "22005",
                    format!("variable \"{name}\" is declared CONSTANT"),
                ));
            }
            let v = coerce(ctx, v, given_ty, ty)?;
            if let Some(Var::Scalar { v: slot, .. }) = frame.get_mut(&name) {
                *slot = v;
            }
            Ok(())
        }
        (Some(Var::Rec(Some(r))), Some(f)) => {
            let Some(k) = r.cols.iter().position(|c| *c == f) else {
                return Err(PgError::new(
                    code::UNDEFINED_COLUMN,
                    format!("record \"{name}\" has no field \"{f}\""),
                ));
            };
            let v = coerce(ctx, v, given_ty, r.tys[k])?;
            if let Some(Var::Rec(Some(r))) = frame.get_mut(&name) {
                r.vals[k] = v;
            }
            Ok(())
        }
        (Some(Var::Rec(None)), Some(_)) => {
            Err(PgError::new("55000", format!("record \"{name}\" is not assigned yet")))
        }
        _ => Err(PgError::new(code::SYNTAX_ERROR, format!("\"{target}\" is not a known variable"))),
    }
}

/// Assigns a query's first row to INTO targets (one record, or one
/// variable per column).
fn assign_row(
    ctx: &mut Ctx,
    info: &SessionInfo,
    frame: &mut Frame,
    targets: &[String],
    r: &super::engine::StmtResult,
    strict: bool,
) -> PgResult<()> {
    if strict && r.rows.is_empty() {
        return Err(PgError::new("P0002", "query returned no rows"));
    }
    if strict && r.rows.len() > 1 {
        return Err(PgError::new("P0003", "query returned more than one row"));
    }
    let row = r.rows.first().cloned();
    if let [t] = targets
        && let Some(Var::Rec(_)) = frame.get(t)
    {
        let rec = row.map(|vals| Rec {
            cols: r.cols.iter().map(|c| c.name.clone()).collect(),
            tys: r.cols.iter().map(|c| c.ty).collect(),
            vals,
        });
        if let Some(Var::Rec(slot)) = frame.get_mut(t) {
            *slot = rec;
        }
        return Ok(());
    }
    let row = row.unwrap_or_else(|| vec![Value::Null; targets.len()]);
    for (i, t) in targets.iter().enumerate() {
        let v = row.get(i).cloned().unwrap_or(Value::Null);
        let ty = r.cols.get(i).map(|c| c.ty);
        assign(ctx, info, frame, t, v, ty)?;
    }
    Ok(())
}

fn exec_stmt(ctx: &mut Ctx, info: &SessionInfo, frame: &mut Frame, s: &Stmt) -> PgResult<Flow> {
    match s {
        Stmt::Null => Ok(Flow::Normal),
        Stmt::Block(b) => exec_block(ctx, info, frame, b),
        Stmt::Assign { target, expr } => {
            let ty = match frame.get(target.split('.').next().unwrap_or(target)) {
                Some(Var::Scalar { ty, .. }) if !target.contains('.') => Some(*ty),
                _ => None,
            };
            let (sql, params, types) = substitute(frame, expr)?;
            let r = run_sql(ctx, info, &format!("SELECT {sql}"), &params, &types)?;
            if r.rows.len() > 1 {
                return Err(PgError::new(
                    code::CARDINALITY_VIOLATION,
                    "query returned more than one row",
                ));
            }
            let v = r.rows.first().and_then(|r| r.first().cloned()).unwrap_or(Value::Null);
            let from = r.cols.first().map(|c| c.ty);
            let _ = ty;
            assign(ctx, info, frame, target, v, from)?;
            Ok(Flow::Normal)
        }
        Stmt::If { arms, els } => {
            for (cond, body) in arms {
                if truthy(&eval_expr(ctx, info, frame, cond, Some(Type::BOOL))?) {
                    return exec_stmts(ctx, info, frame, body);
                }
            }
            match els {
                Some(b) => exec_stmts(ctx, info, frame, b),
                None => Ok(Flow::Normal),
            }
        }
        Stmt::Case { subject, arms, els } => {
            for (cond, body) in arms {
                let test = match subject {
                    Some(sub) => format!("({sub}) IN ({cond})"),
                    None => cond.clone(),
                };
                if truthy(&eval_expr(ctx, info, frame, &test, Some(Type::BOOL))?) {
                    return exec_stmts(ctx, info, frame, body);
                }
            }
            match els {
                Some(b) => exec_stmts(ctx, info, frame, b),
                None => Err(PgError::new("20000", "case not found")),
            }
        }
        Stmt::Loop { label, body } => loop {
            match exec_stmts(ctx, info, frame, body)? {
                Flow::Normal => {}
                f => match loop_control(f, label) {
                    Some(next) => return Ok(next),
                    None => continue,
                },
            }
        },
        Stmt::While { label, cond, body } => {
            while truthy(&eval_expr(ctx, info, frame, cond, Some(Type::BOOL))?) {
                match exec_stmts(ctx, info, frame, body)? {
                    Flow::Normal => {}
                    f => {
                        if let Some(next) = loop_control(f, label) {
                            return Ok(next);
                        }
                    }
                }
            }
            Ok(Flow::Normal)
        }
        Stmt::ForInt { label, var, reverse, lo, hi, by, body } => {
            let int = |v: Value| -> i64 { super::funcs::as_f64(&v) as i64 };
            let lo = int(eval_expr(ctx, info, frame, lo, Some(Type::INT4))?);
            let hi = int(eval_expr(ctx, info, frame, hi, Some(Type::INT4))?);
            let step = match by {
                Some(b) => int(eval_expr(ctx, info, frame, b, Some(Type::INT4))?),
                None => 1,
            };
            if step <= 0 {
                return Err(PgError::new(
                    "22023",
                    "BY value of FOR loop must be greater than zero",
                ));
            }
            frame.scopes.push(HashMap::new());
            let mut i = lo;
            let mut result = Ok(Flow::Normal);
            loop {
                if (!reverse && i > hi) || (*reverse && i < hi) {
                    break;
                }
                frame.scopes.last_mut().unwrap().insert(
                    var.clone(),
                    Var::Scalar { v: Value::Int(i), ty: Type::INT4, constant: false },
                );
                match exec_stmts(ctx, info, frame, body) {
                    Ok(Flow::Normal) => {}
                    Ok(f) => {
                        if let Some(next) = loop_control(f, label) {
                            result = Ok(next);
                            break;
                        }
                    }
                    Err(e) => {
                        result = Err(e);
                        break;
                    }
                }
                i = if *reverse { i - step } else { i + step };
            }
            frame.scopes.pop();
            frame.set_found(lo <= hi);
            result
        }
        Stmt::ForQuery { label, var, query, dynamic, body } => {
            let r = if *dynamic {
                let sql = value_text(&eval_expr(ctx, info, frame, query, Some(Type::TEXT))?);
                run_sql(ctx, info, &sql, &[], &[])?
            } else {
                let (sql, params, types) = substitute(frame, query)?;
                run_sql(ctx, info, &sql, &params, &types)?
            };
            let cols: Vec<String> = r.cols.iter().map(|c| c.name.clone()).collect();
            let tys: Vec<Type> = r.cols.iter().map(|c| c.ty).collect();
            let any = !r.rows.is_empty();
            let declared_scalar = matches!(frame.get(var), Some(Var::Scalar { .. }));
            for row in r.rows {
                let v = if declared_scalar {
                    Var::Scalar {
                        v: row.into_iter().next().unwrap_or(Value::Null),
                        ty: tys.first().copied().unwrap_or(Type::TEXT),
                        constant: false,
                    }
                } else {
                    Var::Rec(Some(Rec { cols: cols.clone(), tys: tys.clone(), vals: row }))
                };
                if declared_scalar {
                    if let Some(slot) = frame.get_mut(var) {
                        *slot = v;
                    }
                } else {
                    frame.scopes.last_mut().unwrap().insert(var.clone(), v);
                }
                match exec_stmts(ctx, info, frame, body)? {
                    Flow::Normal => {}
                    f => {
                        if let Some(next) = loop_control(f, label) {
                            frame.set_found(any);
                            return Ok(next);
                        }
                    }
                }
            }
            frame.set_found(any);
            Ok(Flow::Normal)
        }
        Stmt::ForEach { label, var, array, body } => {
            let arr = eval_expr(ctx, info, frame, array, None)?;
            let items = match arr {
                Value::Array(a) => a.items,
                Value::Null => vec![],
                _ => {
                    return Err(PgError::new(
                        code::DATATYPE_MISMATCH,
                        "FOREACH expression must yield an array",
                    ));
                }
            };
            for item in items {
                let ty = match frame.get(var) {
                    Some(Var::Scalar { ty, .. }) => *ty,
                    _ => types::value_type_guess(&item),
                };
                assign(ctx, info, frame, var, item, Some(ty))?;
                match exec_stmts(ctx, info, frame, body)? {
                    Flow::Normal => {}
                    f => {
                        if let Some(next) = loop_control(f, label) {
                            return Ok(next);
                        }
                    }
                }
            }
            Ok(Flow::Normal)
        }
        Stmt::Exit { cont, label, when } => {
            if let Some(w) = when
                && !truthy(&eval_expr(ctx, info, frame, w, Some(Type::BOOL))?)
            {
                return Ok(Flow::Normal);
            }
            Ok(if *cont { Flow::Continue(label.clone()) } else { Flow::Exit(label.clone()) })
        }
        Stmt::Return(e) => {
            if frame.func.ret.base == Base::Trigger {
                // RETURN NEW / OLD / NULL (or any row variable).
                let name = e.as_deref().map(|s| s.trim().to_ascii_lowercase()).unwrap_or_default();
                frame.ret_row = Some(match frame.get(&name) {
                    Some(Var::Rec(r)) => r.clone(),
                    Some(Var::NullRow(_)) => None,
                    _ if name == "null" || name.is_empty() => None,
                    _ => {
                        return Err(PgError::new(
                            code::DATATYPE_MISMATCH,
                            "returned row structure does not match the structure of the triggering table",
                        ));
                    }
                });
                return Ok(Flow::Return);
            }
            if let Some(e) = e {
                if frame.func.returns_set
                    || frame.func.procedure
                    || frame.func.ret.base == Base::Void
                {
                    return Err(PgError::new(
                        code::SYNTAX_ERROR,
                        "RETURN cannot have a parameter in function returning set or void",
                    ));
                }
                let ty = frame.func.ret;
                frame.ret = Some(eval_expr(ctx, info, frame, e, Some(ty))?);
            }
            Ok(Flow::Return)
        }
        Stmt::ReturnNext(e) => {
            let row = match e {
                Some(e) => match frame.get(e.trim()) {
                    Some(Var::Rec(Some(r))) => r.vals.clone(),
                    _ => vec![eval_expr(ctx, info, frame, e, Some(frame.func.ret))?],
                },
                // `RETURNS TABLE (...)`: the output columns are variables.
                None => frame
                    .func
                    .out_cols
                    .iter()
                    .map(|(n, _)| match frame.get(n) {
                        Some(Var::Scalar { v, .. }) => v.clone(),
                        _ => Value::Null,
                    })
                    .collect(),
            };
            frame.set_rows.push(row);
            Ok(Flow::Normal)
        }
        Stmt::ReturnQuery(q) => {
            let (sql, params, types) = substitute(frame, q)?;
            let r = run_sql(ctx, info, &sql, &params, &types)?;
            frame.set_rows.extend(r.rows);
            Ok(Flow::Normal)
        }
        Stmt::Perform(q) => {
            let (sql, params, types) = substitute(frame, q)?;
            let r = run_sql(ctx, info, &format!("SELECT {sql}"), &params, &types)?;
            frame.row_count = r.rows.len() as i64;
            frame.set_found(!r.rows.is_empty());
            Ok(Flow::Normal)
        }
        Stmt::Sql { sql, into, strict } => {
            let (sql, params, types) = substitute(frame, sql)?;
            let r = run_sql(ctx, info, &sql, &params, &types)?;
            let n = affected_of(&r);
            frame.row_count = n;
            frame.set_found(n > 0);
            if !into.is_empty() {
                assign_row(ctx, info, frame, into, &r, *strict)?;
            }
            Ok(Flow::Normal)
        }
        Stmt::Execute { sql, into, strict, using } => {
            let text = value_text(&eval_expr(ctx, info, frame, sql, Some(Type::TEXT))?);
            let mut params = vec![];
            let mut types = vec![];
            for u in using {
                let (s, p, t) = substitute(frame, u)?;
                let r = run_sql(ctx, info, &format!("SELECT {s}"), &p, &t)?;
                params.push(r.rows.first().and_then(|r| r.first().cloned()).unwrap_or(Value::Null));
                types.push(r.cols.first().map_or(Type::TEXT, |c| c.ty));
            }
            let r = run_sql(ctx, info, &text, &params, &types)?;
            let n = affected_of(&r);
            frame.row_count = n;
            frame.set_found(n > 0);
            if !into.is_empty() {
                assign_row(ctx, info, frame, into, &r, *strict)?;
            }
            Ok(Flow::Normal)
        }
        Stmt::GetDiag(items) => {
            for (var, item) in items {
                let v = match item.as_str() {
                    "row_count" => Value::Int(frame.row_count),
                    _ => Value::Null,
                };
                assign(ctx, info, frame, var, v, Some(Type::INT8))?;
            }
            Ok(Flow::Normal)
        }
        Stmt::Assert { cond, msg } => {
            let v = eval_expr(ctx, info, frame, cond, Some(Type::BOOL))?;
            if !truthy(&v) {
                let m = match msg {
                    Some(m) => value_text(&eval_expr(ctx, info, frame, m, Some(Type::TEXT))?),
                    None => "assertion failed".into(),
                };
                return Err(PgError::new("P0004", m));
            }
            Ok(Flow::Normal)
        }
        Stmt::Raise { level, fmt, args, using, condition } => {
            // `RAISE;` re-raises the error being handled.
            if fmt.is_none() && condition.is_none() && using.is_empty() {
                return Err(frame.handling.clone().unwrap_or_else(|| {
                    PgError::new(
                        "0Z002",
                        "RAISE without parameters cannot be used outside an exception handler",
                    )
                }));
            }
            let mut msg = String::new();
            if let Some(f) = fmt {
                let mut vals = vec![];
                for a in args {
                    vals.push(value_text_or_null(&eval_expr(
                        ctx,
                        info,
                        frame,
                        a,
                        Some(Type::TEXT),
                    )?));
                }
                let mut it = vals.into_iter();
                let mut chars = f.chars().peekable();
                while let Some(ch) = chars.next() {
                    if ch == '%' {
                        if chars.peek() == Some(&'%') {
                            chars.next();
                            msg.push('%');
                        } else {
                            msg.push_str(&it.next().unwrap_or_default());
                        }
                    } else {
                        msg.push(ch);
                    }
                }
            } else if let Some(c) = condition {
                msg = c.strip_prefix("sqlstate:").unwrap_or(c).to_string();
            }
            let mut errcode =
                condition.as_deref().map(condition_code).unwrap_or_else(|| "P0001".into());
            let mut detail = None;
            let mut hint = None;
            for (opt, e) in using {
                let v = value_text(&eval_expr(ctx, info, frame, e, Some(Type::TEXT))?);
                match opt.as_str() {
                    "message" => msg = v,
                    "detail" => detail = Some(v),
                    "hint" => hint = Some(v),
                    "errcode" => errcode = condition_code(&v.to_ascii_lowercase()),
                    _ => {}
                }
            }
            let mut err = PgError::new(Box::leak(errcode.into_boxed_str()), msg);
            err.detail = detail;
            err.hint = hint;
            match level.as_str() {
                "exception" => Err(err),
                lvl => {
                    // Only levels the client sees by default are sent.
                    let sev: &'static str = match lvl {
                        "warning" => "WARNING",
                        "info" => "INFO",
                        "notice" => "NOTICE",
                        _ => return Ok(Flow::Normal),
                    };
                    let mut n = err;
                    n.severity = sev;
                    n.code = if sev == "WARNING" { "01000" } else { "00000" };
                    ctx.rt.notices.push(n);
                    Ok(Flow::Normal)
                }
            }
        }
    }
}

fn affected_of(r: &super::engine::StmtResult) -> i64 {
    if r.returns_rows
        && !r.tag.starts_with("INSERT")
        && !r.tag.starts_with("UPDATE")
        && !r.tag.starts_with("DELETE")
    {
        return r.rows.len() as i64;
    }
    r.tag.rsplit(' ').next().and_then(|n| n.parse().ok()).unwrap_or(r.rows.len() as i64)
}

fn value_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        other => types::to_text(other, types::value_type_guess(other), &types::FmtCtx::default()),
    }
}

fn value_text_or_null(v: &Value) -> String {
    if v.is_null() { "<NULL>".into() } else { value_text(v) }
}

/// What an EXIT / CONTINUE means for the loop labelled `label`: None to
/// continue looping, Some(flow) to leave the loop with `flow`.
fn loop_control(f: Flow, label: &Option<String>) -> Option<Flow> {
    match f {
        Flow::Exit(None) => Some(Flow::Normal),
        Flow::Exit(Some(l)) if Some(&l) == label.as_ref() => Some(Flow::Normal),
        Flow::Continue(None) => None,
        Flow::Continue(Some(l)) if Some(&l) == label.as_ref() => None,
        other => Some(other),
    }
}

// ---------------------------------------------------------------------------
// Triggers.

/// A DML statement's triggering event.
#[derive(Clone, Copy, PartialEq)]
pub enum Event {
    Insert,
    Update,
    Delete,
}

impl Event {
    fn name(self) -> &'static str {
        match self {
            Event::Insert => "INSERT",
            Event::Update => "UPDATE",
            Event::Delete => "DELETE",
        }
    }
}

/// The table's triggers for `event` at `timing`, row- or statement-level,
/// in name order (the order Postgres fires them in). `set_cols` is an
/// UPDATE's assigned columns, for `UPDATE OF`.
pub fn triggers_for(
    ctx: &Ctx,
    table: u32,
    timing: &str,
    event: Event,
    row: bool,
    set_cols: &[usize],
) -> Vec<Trigger> {
    let Some(t) = ctx.db.table(table) else { return vec![] };
    let mut out: Vec<Trigger> = ctx
        .db
        .triggers
        .values()
        .filter(|tr| {
            tr.table == table
                && tr.timing == timing
                && tr.row == row
                && tr.events.iter().any(|e| e == event.name())
                && (event != Event::Update
                    || tr.update_cols.is_empty()
                    || tr
                        .update_cols
                        .iter()
                        .any(|c| t.col_index(c).is_some_and(|i| set_cols.contains(&i))))
        })
        .cloned()
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Fires row trigger `tr` for one row: the row to use (BEFORE triggers may
/// change NEW, or return NULL to skip the row: None).
pub fn fire_row(
    ctx: &mut Ctx,
    tr: &Trigger,
    event: Event,
    old: Option<&Row>,
    new: Option<Row>,
) -> PgResult<Option<Row>> {
    let info = super::dml::session_info(ctx);
    let Some(func) = ctx.db.functions.get(&tr.function).cloned() else {
        return Err(PgError::new(
            code::UNDEFINED_FUNCTION,
            format!("trigger function for \"{}\" does not exist", tr.name),
        ));
    };
    let table = ctx
        .db
        .table(tr.table)
        .cloned()
        .ok_or_else(|| PgError::new(code::UNDEFINED_TABLE, "relation does not exist"))?;
    let cols: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let tys: Vec<Type> = table.columns.iter().map(|c| c.ty).collect();
    let rec = |r: &Row| Rec {
        cols: cols.clone(),
        tys: tys.clone(),
        vals: r[..cols.len().min(r.len())].to_vec(),
    };
    let null_row = Var::NullRow(Rec {
        cols: cols.clone(),
        tys: tys.clone(),
        vals: vec![Value::Null; cols.len()],
    });
    let mut frame = Frame::new(&func);
    let scope = &mut frame.scopes[0];
    scope.insert("new".into(), new.as_ref().map_or(null_row.clone(), |r| Var::Rec(Some(rec(r)))));
    scope.insert("old".into(), old.map_or(null_row, |r| Var::Rec(Some(rec(r)))));
    let text =
        |s: &str| Var::Scalar { v: Value::text(s.to_string()), ty: Type::TEXT, constant: true };
    scope.insert("tg_op".into(), text(event.name()));
    scope.insert("tg_name".into(), text(&tr.name));
    scope.insert("tg_when".into(), text(&tr.timing));
    scope.insert("tg_level".into(), text("ROW"));
    scope.insert("tg_table_name".into(), text(&table.name));
    scope.insert("tg_relname".into(), text(&table.name));
    let schema = ctx.db.schemas.get(&table.schema).map(|s| s.name.clone()).unwrap_or_default();
    scope.insert("tg_table_schema".into(), text(&schema));
    scope.insert(
        "tg_nargs".into(),
        Var::Scalar { v: Value::Int(tr.args.len() as i64), ty: Type::INT4, constant: true },
    );
    scope.insert(
        "tg_argv".into(),
        Var::Scalar {
            v: Value::Array(Box::new(types::Array::new(
                tr.args.iter().map(|a| Value::text(a.clone())).collect(),
            ))),
            ty: Type::array_of(Base::Text),
            constant: true,
        },
    );
    // WHEN (condition) decides whether the function runs at all.
    if let Some(w) = &tr.when
        && !truthy(&eval_expr(ctx, &info, &frame, w, Some(Type::BOOL))?)
    {
        return Ok(new);
    }
    let block = Parser::parse_body(&func.body)?;
    enter()?;
    let flow = exec_block(ctx, &info, &mut frame, &block);
    leave();
    match flow? {
        Flow::Return => {}
        _ => {
            return Err(PgError::new(
                "2F005",
                "control reached end of trigger procedure without RETURN",
            ));
        }
    }
    Ok(frame.ret_row.flatten().map(|r| {
        let mut row = new.or_else(|| old.cloned()).unwrap_or_default();
        for (i, v) in r.vals.into_iter().enumerate() {
            if i < row.len() {
                row[i] = v;
            } else {
                row.push(v);
            }
        }
        row
    }))
}

/// Fires a table's statement-level triggers for `event` at `timing`.
pub fn fire_statement(
    ctx: &mut Ctx,
    table: u32,
    timing: &str,
    event: Event,
    set_cols: &[usize],
) -> PgResult<()> {
    for tr in triggers_for(ctx, table, timing, event, false, set_cols) {
        let info = super::dml::session_info(ctx);
        let Some(func) = ctx.db.functions.get(&tr.function).cloned() else { continue };
        let table_name = ctx.db.table(table).map(|t| t.name.clone()).unwrap_or_default();
        let mut frame = Frame::new(&func);
        let scope = &mut frame.scopes[0];
        let text =
            |s: &str| Var::Scalar { v: Value::text(s.to_string()), ty: Type::TEXT, constant: true };
        scope.insert("new".into(), Var::Rec(None));
        scope.insert("old".into(), Var::Rec(None));
        scope.insert("tg_op".into(), text(event.name()));
        scope.insert("tg_name".into(), text(&tr.name));
        scope.insert("tg_when".into(), text(&tr.timing));
        scope.insert("tg_level".into(), text("STATEMENT"));
        scope.insert("tg_table_name".into(), text(&table_name));
        scope.insert("tg_relname".into(), text(&table_name));
        let block = Parser::parse_body(&func.body)?;
        enter()?;
        let r = exec_block(ctx, &info, &mut frame, &block);
        leave();
        r?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_only_routine_ddl() {
        assert_eq!(rewrite("SELECT 1").unwrap(), None);
        let r = rewrite("CREATE TABLE t (id int); CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql; SELECT 1").unwrap().unwrap();
        assert!(r.contains("CALL noida_plpgsql('CREATE FUNCTION f()"), "{r}");
        assert!(r.contains("CREATE TABLE t (id int);"), "{r}");
        assert!(rewrite("CREATE OR REPLACE VIEW v AS SELECT 1").unwrap().is_none());
        assert!(rewrite("DO $$ BEGIN NULL; END $$").unwrap().unwrap().starts_with("CALL"));
    }

    #[test]
    fn parses_a_body() {
        let b = Parser::parse_body(
            "DECLARE n int := 0; r record; BEGIN
               FOR i IN 1..3 LOOP n := n + i; END LOOP;
               IF n > 5 THEN RAISE NOTICE 'big %', n; ELSIF n < 0 THEN NULL; ELSE n := 1; END IF;
               SELECT count(*) INTO n FROM t WHERE x = n;
               INSERT INTO t (x) VALUES (n) RETURNING id INTO n;
               RETURN n;
             EXCEPTION WHEN unique_violation OR others THEN RETURN -1;
             END",
        )
        .unwrap();
        assert_eq!(b.decls.len(), 2);
        assert_eq!(b.body.len(), 5);
        assert_eq!(b.handlers.len(), 1);
        match &b.body[2] {
            Stmt::Sql { sql, into, .. } => {
                assert_eq!(into, &vec!["n".to_string()]);
                assert!(!sql.to_lowercase().contains("into n"), "{sql}");
            }
            other => panic!("{other:?}"),
        }
    }
}
