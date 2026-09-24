//! Postgres compatibility: the wire protocol, a SQL engine and the
//! catalogs drivers expect. See docs/SERVICE_GUIDE.md.

// PgError carries Postgres's full error fields. It travels by value on the
// rare error path, where its size costs nothing worth the indirection.
#![allow(clippy::result_large_err)]

pub mod auth;
pub mod binder;
pub mod casts;
pub mod catalog;
pub mod datetime;
pub mod ddl;
pub mod dml;
pub mod engine;
pub mod error;
pub mod exec;
pub mod funcs;
pub mod json;
pub mod keywords;
pub mod numeric;
pub mod pgcatalog;
pub mod plan;
pub mod server;
pub mod session;
pub mod sigs;
pub mod types;
pub mod tz;

use std::io;
use std::net::SocketAddr;

use error::{PgError, PgResult, code};
use sqlparser::ast as a;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Parses a SQL string into statements.
pub fn parse_sql(sql: &str) -> PgResult<Vec<a::Statement>> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(syntax_error)
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
