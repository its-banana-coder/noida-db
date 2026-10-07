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
    let after_db = rewrite_create_database(sql)?;
    let sql = after_db.as_deref().unwrap_or(sql);
    let after_ext = rewrite_extension_schema(sql);
    let sql = after_ext.as_deref().unwrap_or(sql);
    let after_frame = rewrite_frame_casts(sql);
    let sql = after_frame.as_deref().unwrap_or(sql);
    let after_cons = rewrite_set_constraints(sql);
    let sql = after_cons.as_deref().unwrap_or(sql);
    let after_django = rewrite_django_ddl(sql);
    let sql = after_django.as_deref().unwrap_or(sql);
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

/// `CREATE DATABASE name [WITH] option [=] value ...`: sqlparser takes no
/// options, and they don't change a noida-db database (always UTF8, C
/// locale), so they're checked and dropped. An encoding other than UTF8 is
/// an error rather than silently ignored.
fn rewrite_create_database(sql: &str) -> PgResult<Option<String>> {
    let re = regex_lite::Regex::new(
        r#"(?is)^\s*create\s+database\s+("(?:[^"]|"")+"|[a-z_][a-z0-9_$]*)\s+(?:with\s+)?(.+?)\s*;?\s*$"#,
    )
    .expect("regex");
    let Some(m) = re.captures(sql) else { return Ok(None) };
    let opt = regex_lite::Regex::new(
        r#"(?is)^\s*(connection\s+limit|[a-z_]+)\s*=?\s*('(?:[^']|'')*'|"(?:[^"]|"")*"|-?[a-z0-9_.]+)"#,
    )
    .expect("regex");
    let mut rest = &m[2];
    while !rest.trim().is_empty() {
        let Some(o) = opt.captures(rest) else {
            let near = rest.split_whitespace().next().unwrap_or("");
            return Err(PgError::new(
                code::SYNTAX_ERROR,
                format!("syntax error at or near \"{near}\""),
            ));
        };
        let key = o[1].to_ascii_lowercase();
        let value = o[2].trim_matches(|c| c == '\'' || c == '"').to_string();
        match key.split_whitespace().collect::<Vec<_>>().join(" ").as_str() {
            "encoding" => {
                let norm: String = value
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric())
                    .collect::<String>()
                    .to_ascii_lowercase();
                if !matches!(norm.as_str(), "utf8" | "unicode" | "6" | "default") {
                    let known = matches!(
                        norm.as_str(),
                        "sqlascii"
                            | "latin1"
                            | "latin2"
                            | "latin3"
                            | "latin4"
                            | "latin5"
                            | "latin6"
                            | "latin7"
                            | "latin8"
                            | "latin9"
                            | "latin10"
                            | "win1250"
                            | "win1251"
                            | "win1252"
                            | "win1253"
                            | "win1254"
                            | "win1255"
                            | "win1256"
                            | "win1257"
                            | "win1258"
                            | "win866"
                            | "win874"
                            | "koi8r"
                            | "koi8u"
                            | "eucjp"
                            | "euccn"
                            | "euckr"
                            | "euctw"
                            | "eucjis2004"
                            | "iso88595"
                            | "iso88596"
                            | "iso88597"
                            | "iso88598"
                            | "mulecode"
                            | "sjis"
                            | "big5"
                            | "gbk"
                            | "uhc"
                            | "gb18030"
                            | "johab"
                            | "shiftjis2004"
                    ) || norm.chars().all(|c| c.is_ascii_digit());
                    return Err(if known {
                        PgError::new(
                            code::FEATURE_NOT_SUPPORTED,
                            format!(
                                "encoding \"{value}\" is not supported: noida-db databases are UTF8"
                            ),
                        )
                    } else {
                        PgError::new(
                            code::UNDEFINED_OBJECT,
                            format!("{value} is not a valid encoding name"),
                        )
                    });
                }
            }
            "owner" | "template" | "locale" | "lc_collate" | "lc_ctype" | "icu_locale"
            | "locale_provider" | "collation_version" | "tablespace" | "allow_connections"
            | "connection limit" | "is_template" | "oid" | "strategy" | "builtin_locale"
            | "icu_rules" => {}
            other => {
                return Err(PgError::new(
                    code::SYNTAX_ERROR,
                    format!("option \"{other}\" not recognized"),
                ));
            }
        }
        rest = &rest[o[0].len()..];
    }
    Ok(Some(format!("CREATE DATABASE {}", &m[1])))
}

/// `CREATE EXTENSION x SCHEMA s`: sqlparser wants `WITH SCHEMA`, which
/// Postgres treats the same.
fn rewrite_extension_schema(sql: &str) -> Option<String> {
    let re = regex_lite::Regex::new(r"(?is)^(\s*create\s+extension\s+(?:if\s+not\s+exists\s+)?(?:\x22[^\x22]+\x22|\S+))\s+schema\b")
        .expect("regex");
    re.is_match(sql).then(|| re.replace(sql, "$1 WITH SCHEMA").into_owned())
}

/// `'1 year'::interval PRECEDING` in a window frame, which sqlparser
/// doesn't parse, as `CAST('1 year' AS interval) PRECEDING`.
fn rewrite_frame_casts(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("preceding") || lower.contains("following")) || !sql.contains("::") {
        return None;
    }
    let re = regex_lite::Regex::new(
        r"(?is)('(?:[^']|'')*')::([a-z_][a-z0-9_]*(?:\s+[a-z_][a-z0-9_]*)*?)(\s+(?:preceding|following)\b)",
    )
    .expect("regex");
    let out = re.replace_all(sql, "CAST($1 AS $2)$3").into_owned();
    (out != sql).then_some(out)
}

/// `SET CONSTRAINTS {ALL | name, ...} {DEFERRED | IMMEDIATE}`, which
/// sqlparser doesn't parse: carried as `SET noida_set_constraints =
/// 'names|mode'` to the SET handler (`Engine::set_constraints`).
fn rewrite_set_constraints(sql: &str) -> Option<String> {
    let re = regex_lite::Regex::new(
        r"(?is)\bset\s+constraints\s+(all|[a-z_\x22][a-z0-9_$.\x22\s,]*?)\s+(deferred|immediate)\b",
    )
    .expect("regex");
    if !re.is_match(sql) {
        return None;
    }
    Some(
        re.replace_all(sql, |c: &regex_lite::Captures| {
            let names: Vec<String> = c[1]
                .split(',')
                .map(|n| {
                    let n = n.trim();
                    match n.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
                        Some(q) => q.replace("''", "'"),
                        None => n.to_ascii_lowercase(),
                    }
                })
                .collect();
            format!(
                "SET noida_set_constraints = '{}|{}'",
                names.join(",").replace('\'', "''"),
                c[2].to_ascii_lowercase()
            )
        })
        .into_owned(),
    )
}

/// Clauses Django's Postgres backend emits that sqlparser doesn't take:
/// - `ALTER COLUMN c DROP IDENTITY [IF EXISTS]` -> a marked SET DEFAULT
///   (handled by `Ddl::alter_op`);
/// - `ALTER COLUMN c TYPE t COLLATE "x"` -> the COLLATE dropped (noida-db
///   compares text bytewise, as the C collation);
/// - `CREATE INDEX ... TABLESPACE ts` -> TABLESPACE dropped;
/// - `FOR NO KEY UPDATE` / `FOR KEY SHARE` -> `FOR UPDATE` / `FOR SHARE`,
///   and `FOR ... OF a, b` -> `OF a` (row locks are table-wide here).
fn rewrite_django_ddl(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !["identity", "collate", "tablespace", " key ", " of "].iter().any(|k| lower.contains(k)) {
        return None;
    }
    let mut out = sql.to_string();
    let rules: [(&str, &str); 6] = [
        (r"(?i)\bdrop\s+identity\s+if\s+exists\b", "SET DEFAULT noida_drop_identity(true)"),
        (r"(?i)\bdrop\s+identity\b", "SET DEFAULT noida_drop_identity(false)"),
        (r#"(?i)(\btype\s+[a-z0-9_ ()\[\],."]+?)\s+collate\s+("[^"]+"|[a-z0-9_.]+)"#, "$1"),
        (
            r#"(?i)^(\s*create\s+(?:unique\s+)?index\b[^;]*?)\s+tablespace\s+("[^"]+"|[a-z0-9_]+)"#,
            "$1",
        ),
        (r"(?i)\bfor\s+no\s+key\s+update\b", "FOR UPDATE"),
        (r"(?i)\bfor\s+key\s+share\b", "FOR SHARE"),
    ];
    for (pat, rep) in rules {
        let re = regex_lite::Regex::new(pat).expect("regex");
        out = re.replace_all(&out, rep).into_owned();
    }
    let of_list = regex_lite::Regex::new(r#"(?i)(\bfor\s+(?:update|share)\s+of\s+("[^"]+"|[a-z0-9_.]+))(\s*,\s*("[^"]+"|[a-z0-9_.]+))+"#)
        .expect("regex");
    out = of_list.replace_all(&out, "$1").into_owned();
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
