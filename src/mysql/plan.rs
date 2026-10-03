use super::catalog::{DbState, Table, UniqueKey};
use super::types::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// The five aggregate functions this engine understands. `CountStar` is
/// kept distinct from `Count` because `COUNT(*)` counts rows regardless of
/// NULLs while `COUNT(expr)` counts only the non-NULL evaluations of
/// `expr` — the two need different evaluation logic downstream.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `GROUP_CONCAT`; its argument is a `GROUP_CONCAT` call (see the binder).
    GroupConcat,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Const(Value),
    Param(usize),
    Col(usize),
    ColName(String),
    /// `*` or `table.*` -- this engine only ever binds a single table into
    /// scope per query, so a qualifier (if any) is dropped the same way
    /// `AstExpr::CompoundIdentifier` already is. Expands to every column
    /// of the current row at `Plan::Project` execution time (see
    /// `Executor`'s own handling), not evaluated as a single value the
    /// way every other `Expr` variant is.
    Wildcard,
    /// `FOUND_ROWS()` -- the count `SQL_CALC_FOUND_ROWS` computed for the
    /// most recent query that used it, on this connection, persisting
    /// across intervening statements the same way MySQL's own
    /// `LAST_INSERT_ID()` does (a real SQL function, not the wire
    /// protocol field). See `Plan::Finish`'s `calc_found_rows` and
    /// `Executor::last_found_rows`.
    FoundRows,
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Compare {
        op: CmpOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Arith {
        op: ArithOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Agg {
        func: AggFunc,
        arg: Option<Box<Expr>>,
        /// `COUNT(DISTINCT x)`, `SUM(DISTINCT x)`, ...: duplicates among the
        /// group's non-NULL values are dropped before aggregating.
        distinct: bool,
    },
    SysVar(String),
    /// `expr [NOT] IN (list...)`. Real MySQL's three-valued semantics:
    /// true if `expr` equals any non-NULL list element, else NULL if
    /// `expr` or any list element is NULL, else false -- `negated` flips
    /// true/false but leaves NULL as NULL (matching `NOT (... IS NULL)`).
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `NOT expr` -- three-valued: `NOT NULL` is `NULL`, not `1`.
    Not(Box<Expr>),
    /// `expr IS [NOT] NULL`. The `bool` is `true` for `IS NOT NULL`.
    IsNull(Box<Expr>, bool),
    /// `expr [NOT] LIKE pattern [ESCAPE esc]`. `%`/`_` are wildcards in
    /// `pattern`; `esc` (default `\`, matching MySQL's own implicit
    /// default escape character when no `ESCAPE` clause is given) can
    /// precede either wildcard to match it literally. Matching is
    /// case-insensitive, matching MySQL's default `_ci` collations.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Box<Expr>,
        negated: bool,
    },
    /// Searched `CASE WHEN cond1 THEN r1 WHEN cond2 THEN r2 ... [ELSE e] END`.
    /// A simple `CASE operand WHEN v THEN r ... END` is rewritten by the
    /// binder into this same shape, with each condition being
    /// `operand = v`.
    Case {
        conditions: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
}

/// True if `expr` contains an aggregate function call anywhere within it
/// (e.g. `COUNT(x) + 1`). Used by the binder to decide whether a `SELECT`
/// needs a `Plan::Aggregate` instead of a plain `Plan::Project`.
pub fn contains_agg(expr: &Expr) -> bool {
    match expr {
        Expr::Agg { .. } => true,
        Expr::And(v) | Expr::Or(v) => v.iter().any(contains_agg),
        Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
            contains_agg(left) || contains_agg(right)
        }
        Expr::Call { args, .. } => args.iter().any(contains_agg),
        Expr::InList { expr, list, .. } => contains_agg(expr) || list.iter().any(contains_agg),
        Expr::Not(e) => contains_agg(e),
        Expr::IsNull(e, _) => contains_agg(e),
        Expr::Like { expr, pattern, escape, .. } => {
            contains_agg(expr) || contains_agg(pattern) || contains_agg(escape)
        }
        Expr::Case { conditions, else_result } => {
            conditions.iter().any(|(c, r)| contains_agg(c) || contains_agg(r))
                || else_result.as_ref().is_some_and(|e| contains_agg(e))
        }
        _ => false,
    }
}

/// Where a `Plan::Finish` sort key's value lives in an output row.
#[derive(Clone, Debug, PartialEq)]
pub enum SortKey {
    /// A 0-based visible output column (`ORDER BY 2` when the column
    /// count isn't known until `*` is expanded at execution time).
    Output(usize),
    /// The `n`th hidden trailing sort column.
    Hidden(usize),
}

/// What an `INSERT` does when a row would duplicate a PRIMARY/UNIQUE key.
#[derive(Clone, Debug, PartialEq)]
pub enum InsertMode {
    /// Plain `INSERT`: error 1062.
    Error,
    /// `INSERT IGNORE`: skip the row.
    Ignore,
    /// `REPLACE INTO`: delete the conflicting row(s), then insert.
    Replace,
    /// `INSERT ... ON DUPLICATE KEY UPDATE col = expr, ...`: update the
    /// existing row instead. `VALUES(col)` in an `expr` is the value the
    /// row would have been inserted with.
    Upsert(Vec<(String, Expr)>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum JoinOp {
    Cross,
    Inner(Expr),
    Left(Expr),
}

use crate::mysql::catalog::Column;

#[derive(Clone, Debug, PartialEq)]
pub enum Plan {
    Dummy, // SELECT 1
    ShowDatabases,
    /// `SHOW [FULL] TABLES [FROM db] [LIKE 'p']`.
    ShowTables {
        db: String,
        like: Option<String>,
        full: bool,
    },
    ShowColumns {
        db: String,
        table: String,
    },
    ShowCreateTable {
        db: String,
        table: String,
    },
    Use(String),
    Filter {
        source: Box<Plan>,
        predicate: Expr,
    },
    Project {
        source: Box<Plan>,
        exprs: Vec<Expr>,
        names: Vec<String>,
    },
    Scan {
        db: String,
        table: String,
        /// `FROM orders o`: the name this table's columns are qualified by
        /// in a join (`o.id`), instead of the table's own name.
        alias: Option<String>,
    },
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        op: JoinOp,
    },
    CreateTable {
        db: String,
        table: String,
        columns: Vec<Column>,
        unique_keys: Vec<UniqueKey>,
        if_not_exists: bool,
    },
    /// `DROP TABLE [IF EXISTS] a, b`.
    DropTable {
        tables: Vec<(String, String)>,
        if_exists: bool,
    },
    /// `TRUNCATE [TABLE] t`: removes every row and resets AUTO_INCREMENT.
    Truncate {
        db: String,
        table: String,
    },
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    /// `CREATE INDEX` is accepted and validated (the table and every named
    /// column must exist) but doesn't build a real index -- matching this
    /// engine's existing "simple over performant" tradeoff for lookups
    /// (see `docs/BENCHMARKING.md`'s own note that Postgres/MySQL have no
    /// real indexing yet). What matters for compatibility is that the
    /// *statement* succeeds instead of hard-failing a migration that
    /// issues it.
    CreateIndex {
        db: String,
        table: String,
        columns: Vec<String>,
        if_not_exists: bool,
    },
    Insert {
        db: String,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
        mode: InsertMode,
    },
    /// Single-table `UPDATE`, including MySQL's `ORDER BY ... LIMIT n`.
    Update {
        db: String,
        table: String,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
        order: Vec<(Expr, bool)>,
        limit: Option<u64>,
    },
    /// Single-table `DELETE`, including MySQL's `ORDER BY ... LIMIT n`.
    Delete {
        db: String,
        table: String,
        selection: Option<Expr>,
        order: Vec<(Expr, bool)>,
        limit: Option<u64>,
    },
    /// `GROUP BY` (possibly implicit, i.e. an empty `group_exprs` with at
    /// least one aggregate in `exprs` — the whole input is then one
    /// group). `exprs`/`names` play the same role as in `Project`, except
    /// each expr may additionally contain `Expr::Agg`.
    Aggregate {
        source: Box<Plan>,
        group_exprs: Vec<Expr>,
        exprs: Vec<Expr>,
        names: Vec<String>,
        /// `HAVING`: evaluated per-group, the same way each of `exprs` is
        /// (so it can reference an aggregate function or a grouped column
        /// directly, e.g. `HAVING COUNT(*) > 2`), filtering out groups
        /// where it's false/NULL -- same truthiness rule as `Filter`'s own
        /// `WHERE` predicate. Referencing a `SELECT`-list alias instead of
        /// repeating the aggregate expression (`HAVING cnt > 2` where
        /// `cnt` is the projection's own alias) isn't resolved yet; only
        /// a direct expression works.
        having: Option<Expr>,
    },
    /// `ORDER BY` / `DISTINCT` / `LIMIT` / `OFFSET` / `SQL_CALC_FOUND_ROWS`,
    /// applied to the *output* rows of the `Project`/`Aggregate` it wraps --
    /// after aggregation, the way a real database does it. Found via
    /// testing before a public release: these used to run on the input
    /// rows *before* projection/aggregation, so `SELECT COUNT(*) FROM t
    /// LIMIT 1` counted one row, `GROUP BY ... LIMIT 2` grouped only the
    /// first two input rows, `ORDER BY <alias>`/`ORDER BY 2` didn't sort,
    /// and `DISTINCT` was never applied at all.
    ///
    /// Every `ORDER BY` key that isn't a plain output position is
    /// evaluated by the source itself as an extra trailing *hidden*
    /// column (so a key can be any expression: a column not in the SELECT
    /// list, an alias, an aggregate like `COUNT(*)`); those `hidden`
    /// trailing columns are dropped before the rows reach the client.
    Finish {
        source: Box<Plan>,
        order: Vec<(SortKey, bool)>, // (key, ascending)
        hidden: usize,
        distinct: bool,
        limit: Option<u64>,
        offset: Option<u64>,
        /// Real MySQL's `SQL_CALC_FOUND_ROWS` select modifier: when set,
        /// the row count *before* `limit`/`offset` truncation is recorded
        /// for a later `FOUND_ROWS()` call to read.
        calc_found_rows: bool,
    },
}

/// Walks a bound `Plan` to find how many distinct positional `?`
/// parameters it references, so `COM_STMT_PREPARE`'s response can report
/// the real count and `COM_STMT_EXECUTE` knows how many values to decode
/// from the wire. Placeholders are bound in left-to-right encounter order
/// starting at 0 (see `Binder::param_counter`), so the highest `Expr::Param`
/// index + 1 is exactly the parameter count.
pub fn count_params(plan: &Plan) -> usize {
    fn expr_max(e: &Expr, max: &mut usize) {
        match e {
            Expr::Param(i) => {
                if *i + 1 > *max {
                    *max = *i + 1;
                }
            }
            Expr::And(v) | Expr::Or(v) => {
                for x in v {
                    expr_max(x, max);
                }
            }
            Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
                expr_max(left, max);
                expr_max(right, max);
            }
            Expr::Call { args, .. } => {
                for a in args {
                    expr_max(a, max);
                }
            }
            Expr::Agg { arg: Some(a), .. } => expr_max(a, max),
            Expr::InList { expr, list, .. } => {
                expr_max(expr, max);
                for e in list {
                    expr_max(e, max);
                }
            }
            Expr::Not(e) | Expr::IsNull(e, _) => expr_max(e, max),
            Expr::Like { expr, pattern, escape, .. } => {
                expr_max(expr, max);
                expr_max(pattern, max);
                expr_max(escape, max);
            }
            Expr::Case { conditions, else_result } => {
                for (c, r) in conditions {
                    expr_max(c, max);
                    expr_max(r, max);
                }
                if let Some(e) = else_result {
                    expr_max(e, max);
                }
            }
            _ => {}
        }
    }
    fn plan_max(p: &Plan, max: &mut usize) {
        match p {
            Plan::Filter { source, predicate } => {
                plan_max(source, max);
                expr_max(predicate, max);
            }
            Plan::Project { source, exprs, .. } => {
                plan_max(source, max);
                for e in exprs {
                    expr_max(e, max);
                }
            }
            Plan::Aggregate { source, group_exprs, exprs, .. } => {
                plan_max(source, max);
                for e in group_exprs {
                    expr_max(e, max);
                }
                for e in exprs {
                    expr_max(e, max);
                }
            }
            Plan::Join { left, right, op } => {
                plan_max(left, max);
                plan_max(right, max);
                match op {
                    JoinOp::Inner(e) | JoinOp::Left(e) => expr_max(e, max),
                    JoinOp::Cross => {}
                }
            }
            Plan::Insert { rows, mode, .. } => {
                for r in rows {
                    for e in r {
                        expr_max(e, max);
                    }
                }
                if let InsertMode::Upsert(assignments) = mode {
                    for (_, e) in assignments {
                        expr_max(e, max);
                    }
                }
            }
            Plan::Update { assignments, selection, order, .. } => {
                for (_, e) in assignments {
                    expr_max(e, max);
                }
                if let Some(s) = selection {
                    expr_max(s, max);
                }
                for (e, _) in order {
                    expr_max(e, max);
                }
            }
            Plan::Delete { selection, order, .. } => {
                if let Some(s) = selection {
                    expr_max(s, max);
                }
                for (e, _) in order {
                    expr_max(e, max);
                }
            }
            Plan::Finish { source, .. } => plan_max(source, max),
            _ => {}
        }
    }
    let mut max = 0;
    plan_max(plan, &mut max);
    max
}

/// The real column names a `Plan`'s result set should report on the wire,
/// derived from the plan itself rather than the executed rows (so the
/// server doesn't need to thread names through `Executor::execute_plan`'s
/// recursive row-producing path). Any real client that accesses a row by
/// column name (PHP's `mysqli` -- what WordPress's `$wpdb` uses via its
/// `stdClass` rows -- PDO's associative fetch mode, any ORM) depends on
/// these being real: a placeholder like `"col0"` silently breaks every
/// such access with no error, just a missing property.
pub fn column_names(plan: &Plan, db: &DbState) -> Vec<String> {
    match plan {
        Plan::Project { source, exprs, names } | Plan::Aggregate { source, exprs, names, .. } => {
            let mut out = Vec::with_capacity(names.len());
            for (expr, name) in exprs.iter().zip(names) {
                if matches!(expr, Expr::Wildcard)
                    && let Some(table) = source_table(source, db)
                {
                    out.extend(table.columns.iter().map(|c| c.name.clone()));
                    continue;
                }
                out.push(name.clone());
            }
            out
        }
        Plan::Filter { source, .. } | Plan::Finish { source, .. } => column_names(source, db),
        Plan::ShowDatabases => vec!["Database".to_string()],
        Plan::ShowTables { db: db_name, full, .. } => {
            let mut v = vec![format!("Tables_in_{db_name}")];
            if *full {
                v.push("Table_type".into());
            }
            v
        }
        Plan::ShowColumns { .. } => ["Field", "Type", "Null", "Key", "Default", "Extra"]
            .into_iter()
            .map(String::from)
            .collect(),
        Plan::ShowCreateTable { .. } => {
            vec!["Table".to_string(), "Create Table".to_string()]
        }
        // Not meaningful as a top-level SELECT's own output (a bare
        // `Scan`/`Join`/`Dummy` is always wrapped by a `Project` in
        // practice, and the DDL/DML variants don't return rows at all).
        _ => Vec::new(),
    }
}

/// Walks down to the single table a (non-`Join`) plan scans, for
/// expanding `Expr::Wildcard` into real column names. `None` for
/// anything this engine can't resolve a single source table for (a
/// `Join`'s two tables would be ambiguous, so wildcards aren't expanded
/// there -- `column_names` falls back to `"*"`, which `server.rs`
/// already treats as "no better name" the same way it does `"?"`).
fn source_table(plan: &Plan, db: &DbState) -> Option<std::sync::Arc<Table>> {
    match plan {
        Plan::Scan { db: db_name, table, .. } => {
            crate::mysql::infoschema::lookup_table(db, db_name, table)
        }
        Plan::Filter { source, .. } | Plan::Finish { source, .. } => source_table(source, db),
        _ => None,
    }
}

/// Rebuilds `e` bottom-up, passing every `ColName` through `f`.
pub fn map_colnames(e: Expr, f: &dyn Fn(String) -> Expr) -> Expr {
    let m = |x: Expr| map_colnames(x, f);
    let mb = |x: Box<Expr>| Box::new(map_colnames(*x, f));
    match e {
        Expr::ColName(n) => f(n),
        Expr::And(v) => Expr::And(v.into_iter().map(m).collect()),
        Expr::Or(v) => Expr::Or(v.into_iter().map(m).collect()),
        Expr::Compare { op, left, right } => Expr::Compare { op, left: mb(left), right: mb(right) },
        Expr::Arith { op, left, right } => Expr::Arith { op, left: mb(left), right: mb(right) },
        Expr::Call { name, args } => Expr::Call { name, args: args.into_iter().map(m).collect() },
        Expr::Agg { func, arg, distinct } => Expr::Agg { func, arg: arg.map(mb), distinct },
        Expr::InList { expr, list, negated } => {
            Expr::InList { expr: mb(expr), list: list.into_iter().map(m).collect(), negated }
        }
        Expr::Not(x) => Expr::Not(mb(x)),
        Expr::IsNull(x, neg) => Expr::IsNull(mb(x), neg),
        Expr::Like { expr, pattern, escape, negated } => {
            Expr::Like { expr: mb(expr), pattern: mb(pattern), escape: mb(escape), negated }
        }
        Expr::Case { conditions, else_result } => Expr::Case {
            conditions: conditions.into_iter().map(|(c, r)| (m(c), m(r))).collect(),
            else_result: else_result.map(mb),
        },
        other => other,
    }
}
