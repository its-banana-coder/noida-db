//! Executes parsed `SELECT` statements against noida-db's built-in
//! `system.one` / `numbers(N)` sources and scalar expressions. No user
//! tables yet (milestone 2).

use super::error::ChError;
use super::sql::{self, Expr, Select, Table};

pub const SERVER_VERSION: &str = "24.8.4.13";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    UInt8,
    UInt64,
    Int64,
    String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    UInt8(u8),
    UInt64(u64),
    Int64(i64),
    Str(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<(String, Type)>,
    pub rows: Vec<Vec<Val>>,
}

/// Parses and runs `sql`, returning the result and the `FORMAT` clause (if
/// any) so the caller can pick an output format.
pub fn execute(sql: &str) -> Result<(QueryResult, Option<String>), ChError> {
    let select = sql::parse(sql)?;
    let format = select.format.clone();
    let result = run(&select)?;
    Ok((result, format))
}

fn run(select: &Select) -> Result<QueryResult, ChError> {
    match &select.table {
        Table::None => run_scalar(select),
        Table::SystemOne => project(select, &system_one()),
        Table::SystemNumbers(n) => project(select, &limited(system_numbers(*n), select.limit)),
        Table::Unknown { database, table } => Err(ChError::unknown_table(database, table)),
    }
}

fn system_one() -> QueryResult {
    QueryResult { columns: vec![("dummy".into(), Type::UInt8)], rows: vec![vec![Val::UInt8(0)]] }
}

fn system_numbers(n: u64) -> QueryResult {
    let rows = (0..n).map(|i| vec![Val::UInt64(i)]).collect();
    QueryResult { columns: vec![("number".into(), Type::UInt64)], rows }
}

fn limited(mut r: QueryResult, limit: Option<u64>) -> QueryResult {
    if let Some(n) = limit {
        r.rows.truncate(n as usize);
    }
    r
}

/// Evaluates the select list once, as scalars (no `FROM`).
fn run_scalar(select: &Select) -> Result<QueryResult, ChError> {
    let mut columns = Vec::with_capacity(select.items.len());
    let mut row = Vec::with_capacity(select.items.len());
    for item in &select.items {
        let (ty, val) = eval_scalar(&item.expr)?;
        let name = item.alias.clone().unwrap_or_else(|| item.expr.default_name());
        columns.push((name, ty));
        row.push(val);
    }
    Ok(QueryResult { columns, rows: vec![row] })
}

fn eval_scalar(e: &Expr) -> Result<(Type, Val), ChError> {
    match e {
        Expr::Int(n) if (0..=255).contains(n) => Ok((Type::UInt8, Val::UInt8(*n as u8))),
        Expr::Int(n) => Ok((Type::Int64, Val::Int64(*n))),
        Expr::Str(s) => Ok((Type::String, Val::Str(s.clone()))),
        Expr::Call(name, args) => eval_call(name, args),
        Expr::Ident(name) => Err(ChError::unknown_identifier(name)),
        Expr::Star => Err(ChError::syntax("SELECT * needs a FROM clause")),
    }
}

fn eval_call(name: &str, args: &[Expr]) -> Result<(Type, Val), ChError> {
    if !args.is_empty() {
        return Err(ChError::not_implemented(&format!("{name}() with arguments")));
    }
    match name.to_ascii_lowercase().as_str() {
        "version" => Ok((Type::String, Val::Str(SERVER_VERSION.into()))),
        "currentdatabase" => Ok((Type::String, Val::Str("default".into()))),
        "hostname" => Ok((Type::String, Val::Str("localhost".into()))),
        "timezone" => Ok((Type::String, Val::Str("UTC".into()))),
        "uptime" => Ok((Type::UInt64, Val::UInt64(0))),
        _ => Err(ChError::unknown_function(name)),
    }
}

/// Projects `select`'s item list onto a source's rows: `*` returns every
/// column, a bare identifier looks up a column by name. Scalar expressions
/// mixed into a `FROM` query aren't needed by any P0 client yet.
fn project(select: &Select, source: &QueryResult) -> Result<QueryResult, ChError> {
    if let [item] = select.items.as_slice()
        && item.expr == Expr::Star
        && item.alias.is_none()
    {
        return Ok(limited(source.clone(), select.limit));
    }
    let mut idxs = Vec::with_capacity(select.items.len());
    let mut columns = Vec::with_capacity(select.items.len());
    for item in &select.items {
        match &item.expr {
            Expr::Ident(name) => {
                let idx = source
                    .columns
                    .iter()
                    .position(|(n, _)| n == name)
                    .ok_or_else(|| ChError::unknown_identifier(name))?;
                idxs.push(idx);
                let out_name = item.alias.clone().unwrap_or_else(|| name.clone());
                columns.push((out_name, source.columns[idx].1));
            }
            other => return Err(ChError::not_implemented(&other.default_name())),
        }
    }
    let rows = source.rows.iter().map(|r| idxs.iter().map(|&i| r[i].clone()).collect()).collect();
    Ok(limited(QueryResult { columns, rows }, select.limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_sql(sql: &str) -> QueryResult {
        execute(sql).unwrap().0
    }

    #[test]
    fn select_one() {
        let r = run_sql("SELECT 1");
        assert_eq!(r.columns, [("1".to_string(), Type::UInt8)]);
        assert_eq!(r.rows, [[Val::UInt8(1)]]);
    }

    #[test]
    fn select_large_int_is_int64() {
        let r = run_sql("SELECT 1000");
        assert_eq!(r.columns, [("1000".to_string(), Type::Int64)]);
    }

    #[test]
    fn select_version() {
        let r = run_sql("SELECT version()");
        assert_eq!(r.columns, [("version()".to_string(), Type::String)]);
        assert_eq!(r.rows, [[Val::Str(SERVER_VERSION.into())]]);
    }

    #[test]
    fn select_aliased() {
        let r = run_sql("SELECT 1 AS one");
        assert_eq!(r.columns, [("one".to_string(), Type::UInt8)]);
    }

    #[test]
    fn select_star_from_system_one() {
        let r = run_sql("SELECT * FROM system.one");
        assert_eq!(r.columns, [("dummy".to_string(), Type::UInt8)]);
        assert_eq!(r.rows, [[Val::UInt8(0)]]);
    }

    #[test]
    fn select_from_numbers_with_limit() {
        let r = run_sql("SELECT number FROM numbers(100) LIMIT 3");
        assert_eq!(r.rows, [[Val::UInt64(0)], [Val::UInt64(1)], [Val::UInt64(2)]]);
    }

    #[test]
    fn select_star_from_numbers() {
        let r = run_sql("SELECT * FROM numbers(5)");
        assert_eq!(r.rows.len(), 5);
    }

    #[test]
    fn unknown_table_errors() {
        let e = execute("SELECT * FROM nope").unwrap_err();
        assert_eq!(e.code, 60);
    }

    #[test]
    fn unknown_function_errors() {
        let e = execute("SELECT nosuchfn()").unwrap_err();
        assert_eq!(e.code, 46);
    }

    #[test]
    fn bare_column_name_with_no_from_is_unknown_identifier() {
        // "SELECT FROM" parses as selecting the bare column `FROM` (see
        // sql::tests::select_from_as_bare_identifier); the engine is what
        // rejects it, the way ClickHouse rejects an unresolvable identifier.
        let e = execute("SELECT FROM").unwrap_err();
        assert_eq!(e.code, 47);
    }

    #[test]
    fn syntax_error_propagates() {
        let e = execute("not sql").unwrap_err();
        assert_eq!(e.code, 62);
    }

    #[test]
    fn format_clause_is_returned() {
        let (_, format) = execute("SELECT 1 FORMAT JSON").unwrap();
        assert_eq!(format, Some("JSON".into()));
    }
}
