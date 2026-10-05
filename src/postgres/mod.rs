//! Postgres compatibility: the wire protocol, a SQL engine and the
//! catalogs drivers expect. See docs/SERVICE_GUIDE.md.

// PgError carries Postgres's full error fields. It travels by value on the
// rare error path, where its size costs nothing worth the indirection.
#![allow(clippy::result_large_err)]

pub mod arrayset;
pub mod auth;
pub mod binder;
pub mod casts;
pub mod catalog;
pub mod copy;
pub mod ddl;
pub mod dml;
pub mod engine;
pub mod error;
pub mod exec;
pub mod fts;
pub mod funcs;
pub mod jsonpath;
pub mod keywords;
pub mod pgcatalog;
pub mod plan;
pub mod plpgsql;
pub mod ranges;
pub mod refresh;
pub mod seqddl;
pub mod server;
pub mod session;
pub mod sigs;
pub mod types;

pub use crate::sql::{datetime, json, numeric, tz};

use std::io;
use std::net::SocketAddr;

use error::{PgError, PgResult, code};
use sqlparser::ast as a;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Parses a SQL string into statements.
pub fn parse_sql(sql: &str) -> PgResult<Vec<a::Statement>> {
    let after_seq = seqddl::rewrite(sql)?;
    let sql1 = after_seq.as_deref().unwrap_or(sql);
    let after_refresh = refresh::rewrite(sql1)?;
    let sql2 = after_refresh.as_deref().unwrap_or(sql1);
    let after_routines = plpgsql::rewrite(sql2)?;
    let sql3 = after_routines.as_deref().unwrap_or(sql2);
    let after_overriding = rewrite_overriding(sql3);
    let sql4 = after_overriding.as_deref().unwrap_or(sql3);
    let after_subscripts = arrayset::rewrite(sql4);
    let sql = after_subscripts.as_deref().unwrap_or(sql4);
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(syntax_error)
}

/// `INSERT ... OVERRIDING SYSTEM VALUE ...`, which sqlparser doesn't
/// parse: the clause is dropped and the statement marked with the
/// otherwise-unused `INSERT OVERWRITE`, which the binder reads as the
/// override. `OVERRIDING USER VALUE` is dropped.
fn rewrite_overriding(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("overriding") {
        return None;
    }
    let re = regex_lite::Regex::new(r"(?is)\binsert\s+into\b(.*?)\boverriding\s+system\s+value\b")
        .ok()?;
    let out = re.replace_all(sql, "INSERT OVERWRITE INTO$1").into_owned();
    let re_user = regex_lite::Regex::new(r"(?is)\boverriding\s+user\s+value\b").ok()?;
    let out = re_user.replace_all(&out, "").into_owned();
    (out != sql).then_some(out)
}

/// Parses a single SQL expression (defaults, check constraints).
pub fn parse_expr(sql: &str) -> PgResult<a::Expr> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).map_err(syntax_error)?;
    parser.parse_expr().map_err(syntax_error)
}

fn syntax_error(e: sqlparser::parser::ParserError) -> PgError {
    let msg = match &e {
        sqlparser::parser::ParserError::TokenizerError(m) => m.clone(),
        sqlparser::parser::ParserError::ParserError(m) => m.clone(),
        sqlparser::parser::ParserError::RecursionLimitExceeded => "statement too complex".into(),
    };
    // "Expected: X, found: Y at Line: 1, Column: 8" → Postgres's wording.
    let near =
        msg.split("found: ").nth(1).map(|s| s.split(" at Line").next().unwrap_or(s).to_string());
    match near {
        Some(tok) => PgError::new(code::SYNTAX_ERROR, format!("syntax error at or near \"{tok}\"")),
        None => PgError::new(code::SYNTAX_ERROR, format!("syntax error: {msg}")),
    }
}

/// Binds `addr` and serves Postgres on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    server::spawn(addr)
}
