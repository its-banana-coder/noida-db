//! `CREATE SEQUENCE` and `ALTER SEQUENCE`. sqlparser wants the options in
//! one fixed order and has no `ALTER SEQUENCE` at all, while Postgres takes
//! them in any order, so these two statements are parsed here.
//!
//! `parse_sql` turns each one into `CALL noida_seq_ddl('<its text>')` so it
//! travels through the engine like any other statement; the engine reparses
//! the text with [`parse`] when it runs it.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::error::{PgError, PgResult, code};

/// The name the rewritten statement calls.
pub const CALL_NAME: &str = "noida_seq_ddl";

#[derive(Debug, Default, PartialEq)]
pub struct SeqDdl {
    /// `CREATE` (else `ALTER`).
    pub create: bool,
    /// `IF NOT EXISTS` for create, `IF EXISTS` for alter.
    pub if_flag: bool,
    pub name: Vec<String>,
    /// `AS smallint|integer|bigint`, lowercased.
    pub as_type: Option<String>,
    pub increment: Option<i64>,
    /// `Some(None)` is `NO MINVALUE`.
    pub min: Option<Option<i64>>,
    pub max: Option<Option<i64>>,
    pub start: Option<i64>,
    /// `RESTART` (`Some(None)`) or `RESTART WITH n`.
    pub restart: Option<Option<i64>>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    /// `OWNED BY table.col` (`Some(Some(..))`) or `OWNED BY NONE`.
    pub owned_by: Option<Option<Vec<String>>>,
    pub rename_to: Option<String>,
    pub set_schema: Option<String>,
}

/// Whether the statement starting with these words is one of ours.
fn is_sequence_ddl(words: &[String]) -> bool {
    let w = |i: usize| words.get(i).map(String::as_str);
    match w(0) {
        Some("alter") => w(1) == Some("sequence"),
        Some("create") => {
            let i = if matches!(w(1), Some("temp" | "temporary" | "unlogged")) { 2 } else { 1 };
            w(i) == Some("sequence")
        }
        _ => false,
    }
}

/// Appends the statement `sql[start..end]` to `out`, as a `CALL` when it is
/// sequence DDL.
fn emit(
    sql: &str,
    (start, end): (usize, usize),
    words: &mut Vec<String>,
    out: &mut String,
) -> PgResult<bool> {
    let text = &sql[start..end];
    let ours = is_sequence_ddl(words);
    if ours {
        parse(text)?;
        out.push_str(&format!(" CALL {CALL_NAME}('{}')", text.trim().replace('\'', "''")));
    } else {
        out.push_str(text);
    }
    words.clear();
    Ok(ours)
}

/// Rewrites every `CREATE/ALTER SEQUENCE` in `sql` into a `CALL`, or returns
/// `None` when there is none (the common case, which skips all this work).
pub fn rewrite(sql: &str) -> PgResult<Option<String>> {
    if !sql.to_ascii_lowercase().contains("sequence") {
        return Ok(None);
    }
    // A tokenizer error is reported by the real parser.
    let Ok(toks) = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize_with_location() else {
        return Ok(None);
    };
    // Byte offset of each (line, column) position.
    let mut line_starts = vec![0usize];
    for (i, c) in sql.char_indices() {
        if c == '\n' {
            line_starts.push(i + 1);
        }
    }
    let offset = |loc: sqlparser::tokenizer::Location| -> usize {
        let start = line_starts.get(loc.line as usize - 1).copied().unwrap_or(sql.len());
        sql[start..]
            .char_indices()
            .nth(loc.column as usize - 1)
            .map_or(sql.len(), |(i, _)| start + i)
    };
    let mut out = String::new();
    let mut changed = false;
    let mut stmt_start = 0usize;
    let mut words: Vec<String> = vec![];
    for t in &toks {
        match &t.token {
            Token::SemiColon => {
                let end = offset(t.span.start);
                changed |= emit(sql, (stmt_start, end), &mut words, &mut out)?;
                out.push(';');
                stmt_start = offset(t.span.end);
            }
            Token::Word(w) if words.len() < 3 && w.quote_style.is_none() => {
                words.push(w.value.to_ascii_lowercase());
            }
            _ => {}
        }
    }
    changed |= emit(sql, (stmt_start, sql.len()), &mut words, &mut out)?;
    Ok(changed.then_some(out))
}

fn syntax(near: &str) -> PgError {
    PgError::new(code::SYNTAX_ERROR, format!("syntax error at or near \"{near}\""))
}

struct Cursor {
    toks: Vec<Token>,
    i: usize,
}

impl Cursor {
    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.i)
    }

    fn word(&self) -> Option<String> {
        match self.peek() {
            Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_ascii_lowercase()),
            _ => None,
        }
    }

    /// Consumes the keyword if it is next.
    fn eat(&mut self, kw: &str) -> bool {
        if self.word().as_deref() == Some(kw) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn near(&self) -> String {
        self.peek().map_or("end of input".to_string(), |t| t.to_string())
    }

    fn ident(&mut self) -> PgResult<String> {
        match self.peek() {
            Some(Token::Word(w)) => {
                let s = if w.quote_style.is_some() {
                    w.value.clone()
                } else {
                    w.value.to_ascii_lowercase()
                };
                self.i += 1;
                Ok(s)
            }
            _ => Err(syntax(&self.near())),
        }
    }

    fn name(&mut self) -> PgResult<Vec<String>> {
        let mut parts = vec![self.ident()?];
        while matches!(self.peek(), Some(Token::Period)) {
            self.i += 1;
            parts.push(self.ident()?);
        }
        Ok(parts)
    }

    fn number(&mut self) -> PgResult<i64> {
        let mut neg = false;
        match self.peek() {
            Some(Token::Minus) => {
                neg = true;
                self.i += 1;
            }
            Some(Token::Plus) => self.i += 1,
            _ => {}
        }
        match self.peek() {
            Some(Token::Number(n, _)) => {
                let v: i64 = n.parse().map_err(|_| {
                    PgError::new(
                        code::NUMERIC_VALUE_OUT_OF_RANGE,
                        format!("value \"{n}\" is out of range for type bigint"),
                    )
                })?;
                self.i += 1;
                Ok(if neg { -v } else { v })
            }
            _ => Err(syntax(&self.near())),
        }
    }
}

/// Parses one `CREATE SEQUENCE` or `ALTER SEQUENCE` statement.
pub fn parse(sql: &str) -> PgResult<SeqDdl> {
    let toks = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|e| PgError::new(code::SYNTAX_ERROR, e.to_string()))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
        .collect();
    let mut c = Cursor { toks, i: 0 };
    let mut d = SeqDdl::default();
    if c.eat("create") {
        d.create = true;
        if !(c.eat("temp") || c.eat("temporary")) {
            c.eat("unlogged");
        }
    } else if !c.eat("alter") {
        return Err(syntax(&c.near()));
    }
    if !c.eat("sequence") {
        return Err(syntax(&c.near()));
    }
    if d.create {
        if c.eat("if") {
            if !(c.eat("not") && c.eat("exists")) {
                return Err(syntax(&c.near()));
            }
            d.if_flag = true;
        }
    } else if c.eat("if") {
        if !c.eat("exists") {
            return Err(syntax(&c.near()));
        }
        d.if_flag = true;
    }
    d.name = c.name()?;
    while c.peek().is_some() {
        let Some(w) = c.word() else { return Err(syntax(&c.near())) };
        c.i += 1;
        match w.as_str() {
            "as" => d.as_type = Some(c.ident()?),
            "increment" => {
                c.eat("by");
                d.increment = Some(c.number()?);
            }
            "minvalue" => d.min = Some(Some(c.number()?)),
            "maxvalue" => d.max = Some(Some(c.number()?)),
            "start" => {
                c.eat("with");
                d.start = Some(c.number()?);
            }
            "restart" => {
                c.eat("with");
                let has_number =
                    matches!(c.peek(), Some(Token::Number(..) | Token::Minus | Token::Plus));
                d.restart = Some(if has_number { Some(c.number()?) } else { None });
            }
            "cache" => d.cache = Some(c.number()?),
            "cycle" => d.cycle = Some(true),
            "no" => match c.word().as_deref() {
                Some("minvalue") => {
                    c.i += 1;
                    d.min = Some(None);
                }
                Some("maxvalue") => {
                    c.i += 1;
                    d.max = Some(None);
                }
                Some("cycle") => {
                    c.i += 1;
                    d.cycle = Some(false);
                }
                _ => return Err(syntax(&c.near())),
            },
            "owned" => {
                if !c.eat("by") {
                    return Err(syntax(&c.near()));
                }
                let target = c.name()?;
                d.owned_by = Some(if target == ["none"] { None } else { Some(target) });
            }
            "rename" if !d.create => {
                if !c.eat("to") {
                    return Err(syntax(&c.near()));
                }
                d.rename_to = Some(c.ident()?);
            }
            "set" if !d.create => {
                if c.eat("schema") {
                    d.set_schema = Some(c.ident()?);
                } else if !(c.eat("logged") || c.eat("unlogged")) {
                    return Err(syntax(&c.near()));
                }
            }
            // Ownership and persistence mean nothing to a single-user store.
            "owner" if !d.create => {
                if !c.eat("to") {
                    return Err(syntax(&c.near()));
                }
                c.ident()?;
            }
            _ => {
                c.i -= 1;
                return Err(syntax(&c.near()));
            }
        }
    }
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_in_any_order() {
        let d =
            parse("CREATE SEQUENCE s START WITH 5 INCREMENT BY -2 NO CYCLE MAXVALUE 100").unwrap();
        assert!(d.create);
        assert_eq!(d.name, ["s"]);
        assert_eq!(
            (d.start, d.increment, d.cycle, d.max),
            (Some(5), Some(-2), Some(false), Some(Some(100)))
        );
    }

    #[test]
    fn alter_forms() {
        let d =
            parse("alter sequence if exists public.\"Seq\" restart with 7 owned by t.c").unwrap();
        assert!(!d.create && d.if_flag);
        assert_eq!(d.name, ["public", "Seq"]);
        assert_eq!(d.restart, Some(Some(7)));
        assert_eq!(d.owned_by, Some(Some(vec!["t".into(), "c".into()])));
        assert_eq!(parse("ALTER SEQUENCE s RESTART").unwrap().restart, Some(None));
        assert_eq!(parse("ALTER SEQUENCE s OWNED BY NONE").unwrap().owned_by, Some(None));
        assert_eq!(parse("ALTER SEQUENCE s RENAME TO t").unwrap().rename_to.as_deref(), Some("t"));
    }

    #[test]
    fn syntax_errors_name_the_token() {
        let e = parse("CREATE SEQUENCE s BOGUS 1").unwrap_err();
        assert_eq!(e.message, "syntax error at or near \"BOGUS\"");
    }

    #[test]
    fn rewrite_only_sequence_statements() {
        assert_eq!(rewrite("SELECT 'sequence'").unwrap(), None);
        let r = rewrite("CREATE TABLE t (id int); CREATE SEQUENCE s INCREMENT 2; SELECT 1")
            .unwrap()
            .unwrap();
        assert!(r.contains("CALL noida_seq_ddl('CREATE SEQUENCE s INCREMENT 2')"), "{r}");
        assert!(r.contains("CREATE TABLE t (id int);"), "{r}");
        assert!(r.trim_end().ends_with("SELECT 1"), "{r}");
    }
}
