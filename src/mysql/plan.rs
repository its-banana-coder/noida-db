use super::catalog::{DbState, UniqueKey};
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
    /// A user variable (`@name`), lower-cased.
    UserVar(String),
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
    /// `(SELECT ...)` as a value: NULL with no rows, error 1242 with more
    /// than one. May reference the enclosing query's columns (correlated).
    Subquery(Box<Plan>),
    /// `expr [NOT] IN (SELECT ...)`, with `IN (...)`'s NULL semantics.
    InSubquery {
        expr: Box<Expr>,
        plan: Box<Plan>,
        negated: bool,
    },
    /// `expr op ALL (SELECT ...)` / `expr op ANY|SOME (SELECT ...)`.
    Quantified {
        op: CmpOp,
        expr: Box<Expr>,
        plan: Box<Plan>,
        all: bool,
    },
    /// A window function (`ROW_NUMBER() OVER (...)`, `SUM(x) OVER w`, ...).
    /// `id` is unique within the statement; the executor computes each
    /// window's values over the whole result before evaluating the select
    /// list, and this node reads the current row's value.
    Window {
        id: usize,
        /// Upper-case name; `COUNT(*)` is `COUNT_STAR`.
        func: String,
        args: Vec<Expr>,
        spec: Box<WindowSpec>,
    },
    /// `[NOT] EXISTS (SELECT ...)`.
    Exists {
        plan: Box<Plan>,
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
        Expr::InSubquery { expr, .. } | Expr::Quantified { expr, .. } => contains_agg(expr),
        Expr::Window { args, spec, .. } => window_exprs(args, spec).any(contains_agg),
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

/// Calls `f` on every column name `expr` references, so a query can
/// reject an unknown column before reading any rows (MySQL does, even when
/// the table is empty).
pub fn for_each_colname<'a>(expr: &'a Expr, f: &mut dyn FnMut(&'a str)) {
    match expr {
        Expr::ColName(n) => f(n),
        Expr::And(v) | Expr::Or(v) => v.iter().for_each(|e| for_each_colname(e, f)),
        Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
            for_each_colname(left, f);
            for_each_colname(right, f);
        }
        Expr::Call { args, .. } => args.iter().for_each(|e| for_each_colname(e, f)),
        Expr::Agg { arg: Some(a), .. } => for_each_colname(a, f),
        Expr::InList { expr, list, .. } => {
            for_each_colname(expr, f);
            list.iter().for_each(|e| for_each_colname(e, f));
        }
        Expr::Not(e) | Expr::IsNull(e, _) => for_each_colname(e, f),
        Expr::InSubquery { expr, .. } | Expr::Quantified { expr, .. } => for_each_colname(expr, f),
        Expr::Window { args, spec, .. } => {
            window_exprs(args, spec).for_each(|e| for_each_colname(e, f))
        }
        Expr::Like { expr, pattern, escape, .. } => {
            for_each_colname(expr, f);
            for_each_colname(pattern, f);
            for_each_colname(escape, f);
        }
        Expr::Case { conditions, else_result } => {
            for (c, r) in conditions {
                for_each_colname(c, f);
                for_each_colname(r, f);
            }
            if let Some(e) = else_result {
                for_each_colname(e, f);
            }
        }
        _ => {}
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

/// A window's `PARTITION BY`, `ORDER BY` and frame.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub partition: Vec<Expr>,
    pub order: Vec<(Expr, bool)>, // (key, ascending)
    pub frame: Option<Frame>,
}

/// `ROWS|RANGE BETWEEN start AND end`. `RANGE` bounds are only
/// unbounded or `CURRENT ROW` (the row and its peers).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Frame {
    pub rows: bool,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// Every expression a window node holds, for the generic walkers.
pub fn window_exprs<'a>(args: &'a [Expr], spec: &'a WindowSpec) -> impl Iterator<Item = &'a Expr> {
    args.iter().chain(spec.partition.iter()).chain(spec.order.iter().map(|(e, _)| e))
}

/// Every window node in `expr` (not inside subqueries), each id once.
pub fn collect_windows<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    let mut sub = |e: &'a Expr| collect_windows(e, out);
    match expr {
        Expr::Window { id, .. } => {
            if !out.iter().any(|w| matches!(w, Expr::Window { id: i, .. } if i == id)) {
                out.push(expr);
            }
        }
        Expr::And(v) | Expr::Or(v) | Expr::Call { args: v, .. } => v.iter().for_each(sub),
        Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
            sub(left);
            sub(right);
        }
        Expr::Agg { arg: Some(a), .. } => sub(a),
        Expr::InList { expr, list, .. } => {
            sub(expr);
            list.iter().for_each(sub);
        }
        Expr::Not(e) | Expr::IsNull(e, _) => sub(e),
        Expr::InSubquery { expr, .. } | Expr::Quantified { expr, .. } => sub(expr),
        Expr::Like { expr, pattern, escape, .. } => {
            sub(expr);
            sub(pattern);
            sub(escape);
        }
        Expr::Case { conditions, else_result } => {
            for (c, r) in conditions {
                sub(c);
                sub(r);
            }
            if let Some(e) = else_result {
                sub(e);
            }
        }
        _ => {}
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
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
        indexes: Vec<UniqueKey>,
        foreign_keys: Vec<crate::mysql::catalog::ForeignKey>,
        if_not_exists: bool,
    },
    /// `DROP TABLE [IF EXISTS] a, b`.
    /// `ALTER TABLE t op, op, ...`.
    AlterTable {
        db: String,
        table: String,
        ops: Vec<AlterOp>,
    },
    DropTable {
        tables: Vec<(String, String)>,
        if_exists: bool,
    },
    /// `TRUNCATE [TABLE] t`: removes every row and resets AUTO_INCREMENT.
    Truncate {
        db: String,
        table: String,
    },
    /// `DROP {DATABASE|SCHEMA} [IF EXISTS] name`.
    DropDatabase {
        name: String,
        if_exists: bool,
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
        /// `CREATE UNIQUE INDEX name ...` (enforced, like a UNIQUE key).
        unique: Option<String>,
        /// The index's name (a plain index's too).
        name: String,
    },
    /// `RENAME TABLE a TO b, c TO d`: (db, table, new name) each.
    RenameTables(Vec<(String, String, String)>),
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
        /// A literal or a `?` (`LIMIT ?`); see `Executor::count`.
        limit: Option<Expr>,
    },
    /// Single-table `DELETE`, including MySQL's `ORDER BY ... LIMIT n`.
    Delete {
        db: String,
        table: String,
        selection: Option<Expr>,
        order: Vec<(Expr, bool)>,
        limit: Option<Expr>,
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
        /// `WHERE` predicate. A `SELECT`-list alias (`HAVING cnt > 2`) is
        /// substituted by the binder.
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
    /// A subquery in `FROM` (`(SELECT ...) AS d`) or a reference to a
    /// `WITH` query: the inner rows, addressed by `alias` and `columns`
    /// (the inner query's own names when empty).
    Derived {
        plan: Box<Plan>,
        alias: String,
        columns: Vec<String>,
    },
    /// `UNION [ALL]`, `INTERSECT` and `EXCEPT`. Column names come from the
    /// left side.
    SetOp {
        op: SetOpKind,
        all: bool,
        left: Box<Plan>,
        right: Box<Plan>,
    },
    /// `WITH RECURSIVE name (columns) AS (anchor UNION [ALL] step)`: the
    /// anchor's rows, then `step` run again over the rows the previous
    /// round added (read through `CteRef`) until a round adds none.
    RecursiveCte {
        name: String,
        columns: Vec<String>,
        anchor: Box<Plan>,
        step: Box<Plan>,
        all: bool,
    },
    /// `INSERT ... SELECT`: `query`'s rows become `insert`'s `VALUES`.
    InsertSelect {
        insert: Box<Plan>,
        query: Box<Plan>,
    },
    /// `SELECT ... FOR UPDATE | FOR SHARE [NOWAIT | SKIP LOCKED]`: locks
    /// the rows of `table` matching `predicate` (single-table queries)
    /// before reading.
    Locking {
        source: Box<Plan>,
        /// The locked table and a plan reading the whole rows the query
        /// reads (its FROM, WHERE, ORDER BY and LIMIT, `SELECT *`).
        target: Option<(String, String, Box<Plan>)>,
        exclusive: bool,
        nowait: bool,
        skip_locked: bool,
    },
    /// `UPDATE a JOIN b ON ... SET a.x = b.y, b.z = 1 WHERE ...` (and
    /// `UPDATE a, b SET ...`): `targets` are every table in the join as
    /// (db, table, alias); an assignment's column may be qualified.
    MultiUpdate {
        join: Box<Plan>,
        targets: Vec<(String, String, String)>,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    },
    /// `DELETE a, b FROM a JOIN b ...` / `DELETE FROM a USING a JOIN b ...`:
    /// deletes the matched rows of `targets` only.
    MultiDelete {
        join: Box<Plan>,
        targets: Vec<(String, String, String)>,
        selection: Option<Expr>,
    },
    /// The recursive step's reference to its own CTE.
    CteRef {
        name: String,
        alias: String,
        columns: Vec<String>,
    },
    Finish {
        source: Box<Plan>,
        order: Vec<(SortKey, bool)>, // (key, ascending)
        hidden: usize,
        distinct: bool,
        /// Literals or `?` placeholders (`LIMIT ? OFFSET ?`).
        limit: Option<Expr>,
        offset: Option<Expr>,
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
            Expr::Subquery(p) | Expr::Exists { plan: p, .. } => plan_max(p, max),
            Expr::InSubquery { expr, plan, .. } | Expr::Quantified { expr, plan, .. } => {
                expr_max(expr, max);
                plan_max(plan, max);
            }
            Expr::Window { args, spec, .. } => {
                window_exprs(args, spec).for_each(|e| expr_max(e, max))
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
            Plan::Aggregate { source, group_exprs, exprs, having, .. } => {
                plan_max(source, max);
                for e in group_exprs {
                    expr_max(e, max);
                }
                for e in exprs {
                    expr_max(e, max);
                }
                // Found via testing before a public release: a `?` in
                // HAVING wasn't counted, so the client was told to bind
                // fewer parameters and it evaluated as NULL (no groups).
                having.iter().for_each(|e| expr_max(e, max));
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
            Plan::Update { assignments, selection, order, limit, .. } => {
                limit.iter().for_each(|e| expr_max(e, max));
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
            Plan::Delete { selection, order, limit, .. } => {
                limit.iter().for_each(|e| expr_max(e, max));
                if let Some(s) = selection {
                    expr_max(s, max);
                }
                for (e, _) in order {
                    expr_max(e, max);
                }
            }
            Plan::Finish { source, limit, offset, .. } => {
                plan_max(source, max);
                limit.iter().chain(offset.iter()).for_each(|e| expr_max(e, max));
            }
            Plan::Derived { plan, .. } => plan_max(plan, max),
            Plan::Locking { source, .. } => plan_max(source, max),
            Plan::InsertSelect { insert, query } => {
                plan_max(insert, max);
                plan_max(query, max);
            }
            Plan::SetOp { left, right, .. } => {
                plan_max(left, max);
                plan_max(right, max);
            }
            Plan::RecursiveCte { anchor, step, .. } => {
                plan_max(anchor, max);
                plan_max(step, max);
            }
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
                    && let Some(cols) = source_columns(source, db)
                {
                    out.extend(cols);
                    continue;
                }
                out.push(name.clone());
            }
            out
        }
        Plan::Filter { source, .. }
        | Plan::Finish { source, .. }
        | Plan::Locking { source, .. } => column_names(source, db),
        Plan::SetOp { left, .. } => column_names(left, db),
        Plan::Derived { plan, columns, .. } => {
            if columns.is_empty() {
                column_names(plan, db)
            } else {
                columns.clone()
            }
        }
        Plan::RecursiveCte { columns, .. } | Plan::CteRef { columns, .. } => columns.clone(),
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
fn source_columns(plan: &Plan, db: &DbState) -> Option<Vec<String>> {
    match plan {
        Plan::Scan { db: db_name, table, .. } => {
            crate::mysql::infoschema::lookup_table(db, db_name, table)
                .map(|t| t.columns.iter().map(|c| c.name.clone()).collect())
        }
        Plan::Filter { source, .. } | Plan::Finish { source, .. } => source_columns(source, db),
        Plan::Derived { .. } | Plan::CteRef { .. } => Some(column_names(plan, db)),
        // A joined row is the left table's columns, then the right's.
        Plan::Join { left, right, .. } => {
            let mut cols = source_columns(left, db)?;
            cols.extend(source_columns(right, db)?);
            Some(cols)
        }
        _ => None,
    }
}

/// Output column names known without the catalog (no `*`), for binding
/// a `UNION`'s `ORDER BY name` and a recursive CTE's columns.
pub fn static_names(plan: &Plan) -> Option<Vec<String>> {
    match plan {
        Plan::Project { exprs, names, .. } | Plan::Aggregate { exprs, names, .. } => {
            (!exprs.iter().any(|e| matches!(e, Expr::Wildcard))).then(|| names.clone())
        }
        Plan::Filter { source, .. }
        | Plan::Finish { source, .. }
        | Plan::Locking { source, .. } => static_names(source),
        Plan::SetOp { left, .. } => static_names(left),
        Plan::Derived { plan, columns, .. } => {
            if columns.is_empty() {
                static_names(plan)
            } else {
                Some(columns.clone())
            }
        }
        Plan::RecursiveCte { columns, .. } | Plan::CteRef { columns, .. } => Some(columns.clone()),
        Plan::Dummy => Some(vec![]),
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
        // A subquery's own names belong to its own scope.
        Expr::InSubquery { expr, plan, negated } => {
            Expr::InSubquery { expr: mb(expr), plan, negated }
        }
        Expr::Quantified { op, expr, plan, all } => {
            Expr::Quantified { op, expr: mb(expr), plan, all }
        }
        Expr::Window { id, func, args, spec } => {
            let spec = WindowSpec {
                partition: spec.partition.into_iter().map(m).collect(),
                order: spec.order.into_iter().map(|(e, a)| (m(e), a)).collect(),
                frame: spec.frame,
            };
            Expr::Window { id, func, args: args.into_iter().map(m).collect(), spec: Box::new(spec) }
        }
        other => other,
    }
}

/// Where `ADD`/`MODIFY`/`CHANGE` put a column: `FIRST` or `AFTER col`.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnPos {
    First,
    After(String),
}

/// One `ALTER TABLE` operation.
#[derive(Clone, Debug, PartialEq)]
pub enum AlterOp {
    AddColumn {
        col: Column,
        unique: Vec<UniqueKey>,
        pos: Option<ColumnPos>,
        if_not_exists: bool,
    },
    DropColumn {
        name: String,
        if_exists: bool,
    },
    /// `MODIFY col def` (same name) or `CHANGE old new def`.
    ReplaceColumn {
        old: String,
        col: Column,
        unique: Vec<UniqueKey>,
        pos: Option<ColumnPos>,
    },
    RenameColumn {
        old: String,
        new: String,
    },
    RenameTable(String),
    /// `ADD INDEX`/`ADD KEY` (plain).
    AddIndex(UniqueKey),
    /// `RENAME INDEX|KEY old TO new`.
    RenameKey {
        old: String,
        new: String,
    },
    AddUnique(UniqueKey),
    /// A foreign key; an empty name gets MySQL's `<table>_ibfk_<n>`.
    AddForeignKey(crate::mysql::catalog::ForeignKey),
    DropForeignKey(String),
    AddPrimaryKey(Vec<String>),
    DropKey(String),
    DropPrimaryKey,
    SetDefault {
        col: String,
        default: Option<Value>,
        now: bool,
    },
    AutoIncrement(i64),
    /// Accepted and ignored: foreign keys, plain indexes, ALGORITHM/LOCK.
    Noop,
}
