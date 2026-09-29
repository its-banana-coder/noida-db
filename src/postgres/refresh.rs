//! `REFRESH MATERIALIZED VIEW`. sqlparser 0.63 (checked) has no grammar for
//! it at all, so — like `CREATE`/`ALTER SEQUENCE` in `seqddl.rs` — it's
//! parsed here and rewritten into a `CALL` the engine reparses when it runs.
//! Written from scratch, following PostgreSQL's own `REFRESH MATERIALIZED
//! VIEW` documentation; checked against a real server in
//! `tests/postgres_diff.rs`. No upstream source was copied.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::error::{PgError, PgResult, code};

pub const CALL_NAME: &str = "noida_refresh_matview";

#[derive(Debug, PartialEq)]
pub struct Refresh {
    pub name: Vec<String>,
    /// `WITH NO DATA` clears the view instead of repopulating it.
    pub with_data: bool,
}

fn is_refresh(words: &[String]) -> bool {
    words.first().map(String::as_str) == Some("refresh")
}

fn emit(
    sql: &str,
    (start, end): (usize, usize),
    words: &mut Vec<String>,
    out: &mut String,
) -> PgResult<bool> {
    let text = &sql[start..end];
    let ours = is_refresh(words);
    if ours {
        parse(text)?;
        out.push_str(&format!(" CALL {CALL_NAME}('{}')", text.trim().replace('\'', "''")));
    } else {
        out.push_str(text);
    }
    words.clear();
    Ok(ours)
}

/// Rewrites every `REFRESH MATERIALIZED VIEW` in `sql` into a `CALL`, or
/// `None` when there is none (the common case, which skips all this work).
pub fn rewrite(sql: &str) -> PgResult<Option<String>> {
    if !sql.to_ascii_lowercase().contains("refresh") {
        return Ok(None);
    }
    let Ok(toks) = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize_with_location() else {
        return Ok(None);
    };
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
            Token::Word(w) if words.len() < 2 && w.quote_style.is_none() => {
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
}

/// Parses one `REFRESH MATERIALIZED VIEW` statement.
pub fn parse(sql: &str) -> PgResult<Refresh> {
    let toks = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|e| PgError::new(code::SYNTAX_ERROR, e.to_string()))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::SemiColon))
        .collect();
    let mut c = Cursor { toks, i: 0 };
    if !c.eat("refresh") || !c.eat("materialized") || !c.eat("view") {
        return Err(syntax(&c.near()));
    }
    c.eat("concurrently");
    let name = c.name()?;
    let mut with_data = true;
    if c.eat("with") {
        with_data = !c.eat("no");
        if !c.eat("data") {
            return Err(syntax(&c.near()));
        }
    }
    if c.peek().is_some() {
        return Err(syntax(&c.near()));
    }
    Ok(Refresh { name, with_data })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_options() {
        let r = parse("REFRESH MATERIALIZED VIEW mv").unwrap();
        assert_eq!((r.name, r.with_data), (vec!["mv".into()], true));
        let r = parse("refresh materialized view concurrently s.mv with no data").unwrap();
        assert_eq!((r.name, r.with_data), (vec!["s".into(), "mv".into()], false));
        assert!(parse("REFRESH MATERIALIZED VIEW mv WITH DATA").unwrap().with_data);
    }

    #[test]
    fn rewrite_leaves_other_statements_alone() {
        assert_eq!(rewrite("SELECT 'refresh'").unwrap(), None);
        let r = rewrite("CREATE TABLE t (id int); REFRESH MATERIALIZED VIEW mv; SELECT 1")
            .unwrap()
            .unwrap();
        assert!(r.contains("CALL noida_refresh_matview('REFRESH MATERIALIZED VIEW mv')"), "{r}");
        assert!(r.trim_end().ends_with("SELECT 1"), "{r}");
    }
}
