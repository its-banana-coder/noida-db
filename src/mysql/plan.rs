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
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Const(Value),
    Param(usize),
    Col(usize),
    ColName(String),
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
        _ => false,
    }
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
    ShowTables(String),
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
    },
    Insert {
        db: String,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
    },
    Update {
        db: String,
        table: String,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    },
    Delete {
        db: String,
        table: String,
        selection: Option<Expr>,
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
    },
    /// `ORDER BY`/`LIMIT`/`OFFSET`. Sits between the row source (`Scan`/
    /// `Filter`/`Join`) and the outer `Project`/`Aggregate`, evaluating
    /// `keys` against the *pre-projection* row the same way `Filter`'s own
    /// predicate does (real `ColName` resolution against the source
    /// table's columns) -- this is what lets `ORDER BY` reference a
    /// column that isn't in the `SELECT` list at all (real MySQL allows
    /// this for a non-aggregated query, and real apps rely on it: e.g.
    /// `SELECT id FROM t ORDER BY created_at DESC`). `keys` is empty when
    /// there's a `LIMIT` with no `ORDER BY`. Not meaningful combined with
    /// `Aggregate` when a key references the aggregated result rather
    /// than a `GROUP BY` column -- not yet supported, see binder.
    Sort {
        source: Box<Plan>,
        keys: Vec<(Expr, bool)>, // (expr, ascending)
        limit: Option<u64>,
        offset: Option<u64>,
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
            Plan::Insert { rows, .. } => {
                for r in rows {
                    for e in r {
                        expr_max(e, max);
                    }
                }
            }
            Plan::Update { assignments, selection, .. } => {
                for (_, e) in assignments {
                    expr_max(e, max);
                }
                if let Some(s) = selection {
                    expr_max(s, max);
                }
            }
            Plan::Delete { selection: Some(s), .. } => expr_max(s, max),
            Plan::Sort { source, keys, .. } => {
                plan_max(source, max);
                for (e, _) in keys {
                    expr_max(e, max);
                }
            }
            _ => {}
        }
    }
    let mut max = 0;
    plan_max(plan, &mut max);
    max
}
