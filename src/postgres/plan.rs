//! Bound, typed plans produced by the binder and run by the executor.

use super::types::{Type, Value};

/// A column of a result set, as RowDescription reports it.
#[derive(Clone, Debug, PartialEq)]
pub struct OutCol {
    pub name: String,
    pub ty: Type,
    pub typmod: i32,
    pub table_oid: u32,
    pub attnum: i16,
}

impl OutCol {
    pub fn new(name: impl Into<String>, ty: Type) -> OutCol {
        OutCol { name: name.into(), ty, typmod: -1, table_oid: 0, attnum: 0 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn test(self, o: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            CmpOp::Eq => o == Equal,
            CmpOp::Ne => o != Equal,
            CmpOp::Lt => o == Less,
            CmpOp::Le => o != Greater,
            CmpOp::Gt => o == Greater,
            CmpOp::Ge => o != Less,
        }
    }
    pub fn symbol(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "<>",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SubKind {
    /// `(SELECT ...)` yielding one value.
    Scalar,
    Exists,
    /// `ARRAY(SELECT ...)`.
    Array,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Const(Value),
    Param(usize),
    /// Column of the current row.
    Col(usize),
    /// Column of an enclosing query's row: (levels up, index).
    Outer(usize, usize),
    /// Builtin function or operator, dispatched by name at run time.
    Call {
        name: &'static str,
        args: Vec<Expr>,
        ty: Type,
        arg_tys: Vec<Type>,
    },
    Cast {
        expr: Box<Expr>,
        from: Type,
        to: Type,
        typmod: i32,
        explicit: bool,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>, bool),
    /// IS [NOT] TRUE/FALSE/UNKNOWN: (expr, value to test, negated).
    IsBool(Box<Expr>, Option<bool>, bool),
    Compare {
        op: CmpOp,
        left: Box<Expr>,
        right: Box<Expr>,
        bpchar: bool,
    },
    Distinct {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Coalesce(Vec<Expr>),
    NullIf(Box<Expr>, Box<Expr>),
    Greatest(Vec<Expr>, bool),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `x op ANY/ALL (array)`.
    AnyAll {
        left: Box<Expr>,
        op: CmpOp,
        right: Box<Expr>,
        all: bool,
    },
    /// `x [NOT] IN (SELECT ...)` / `x op ANY/ALL (SELECT ...)`.
    InSub {
        left: Vec<Expr>,
        op: CmpOp,
        query: Box<Query>,
        all: bool,
        negated: bool,
    },
    Sub {
        kind: SubKind,
        query: Box<Query>,
    },
    Array(Vec<Expr>),
    Row(Vec<Expr>),
    /// Placeholders used while binding grouped queries.
    AggRef(usize),
    WinRef(usize),
    /// A column default, evaluated at run time (INSERT ... DEFAULT).
    Default(usize),
}

impl Expr {
    pub fn null() -> Expr {
        Expr::Const(Value::Null)
    }

    /// Calls `f` on each direct child.
    pub fn children_mut(&mut self, f: &mut dyn FnMut(&mut Expr)) {
        match self {
            Expr::Call { args, .. }
            | Expr::And(args)
            | Expr::Or(args)
            | Expr::Coalesce(args)
            | Expr::Greatest(args, _)
            | Expr::Array(args)
            | Expr::Row(args) => args.iter_mut().for_each(f),
            Expr::Cast { expr, .. }
            | Expr::Not(expr)
            | Expr::IsNull(expr, _)
            | Expr::IsBool(expr, ..) => f(expr),
            Expr::Compare { left, right, .. }
            | Expr::Distinct { left, right, .. }
            | Expr::NullIf(left, right) => {
                f(left);
                f(right);
            }
            Expr::AnyAll { left, right, .. } => {
                f(left);
                f(right);
            }
            Expr::Case { operand, whens, else_ } => {
                if let Some(o) = operand {
                    f(o);
                }
                for (w, t) in whens {
                    f(w);
                    f(t);
                }
                if let Some(e) = else_ {
                    f(e);
                }
            }
            Expr::InList { expr, list, .. } => {
                f(expr);
                list.iter_mut().for_each(f);
            }
            Expr::InSub { left, .. } => left.iter_mut().for_each(f),
            Expr::Const(_)
            | Expr::Param(_)
            | Expr::Col(_)
            | Expr::Outer(..)
            | Expr::Sub { .. }
            | Expr::AggRef(_)
            | Expr::WinRef(_)
            | Expr::Default(_) => {}
        }
    }

    pub fn contains(&self, pred: &dyn Fn(&Expr) -> bool) -> bool {
        if pred(self) {
            return true;
        }
        let mut found = false;
        let mut me = self.clone();
        me.children_mut(&mut |c| {
            if !found && c.contains(pred) {
                found = true;
            }
        });
        found
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    /// Index into the projected row.
    pub col: usize,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggCall {
    pub name: &'static str,
    pub args: Vec<Expr>,
    pub arg_tys: Vec<Type>,
    pub ty: Type,
    pub distinct: bool,
    pub filter: Option<Expr>,
    /// ORDER BY inside the call: (expr, desc, nulls_first).
    pub order: Vec<(Expr, bool, bool)>,
    /// `count(*)`.
    pub star: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(Expr),
    CurrentRow,
    Following(Expr),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    pub rows: bool,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WinCall {
    pub name: &'static str,
    pub args: Vec<Expr>,
    pub arg_tys: Vec<Type>,
    pub ty: Type,
    pub partition: Vec<Expr>,
    pub order: Vec<(Expr, bool, bool)>,
    pub frame: Option<Frame>,
    /// Aggregate used as a window function.
    pub agg: Option<AggCall>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, PartialEq)]
pub enum From {
    Table {
        oid: u32,
        ncols: usize,
    },
    /// A pg_catalog / information_schema relation built at run time.
    Virtual {
        name: String,
        ncols: usize,
    },
    Sub(Box<Query>),
    Cte(usize),
    /// Set-returning function in FROM.
    Func {
        name: &'static str,
        args: Vec<Expr>,
        arg_tys: Vec<Type>,
        ncols: usize,
        ordinality: bool,
        lateral: bool,
    },
    /// A single empty row (SELECT without FROM).
    One,
    Join {
        left: Box<From>,
        right: Box<From>,
        kind: JoinKind,
        on: Option<Expr>,
        lateral: bool,
        left_cols: usize,
        right_cols: usize,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Distinct {
    None,
    All,
    /// Indices into the projected row.
    On(Vec<usize>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub from: From,
    pub filter: Option<Expr>,
    /// Group keys (grouped queries).
    pub group: Option<Vec<Expr>>,
    /// Grouping sets as key-index lists (ROLLUP/CUBE/GROUPING SETS).
    pub grouping_sets: Option<Vec<Vec<usize>>>,
    pub aggs: Vec<AggCall>,
    pub having: Option<Expr>,
    pub windows: Vec<WinCall>,
    pub proj: Vec<Expr>,
    /// Leading visible columns; the rest are sort helpers.
    pub visible: usize,
    pub distinct: Distinct,
    pub order: Vec<SortKey>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub with_ties: bool,
    /// SRFs in the select list: projection indices holding set-returning calls.
    pub srf: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CtePlan {
    pub slot: usize,
    pub query: Query,
    pub recursive: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Query {
    Select(Box<Select>),
    Values {
        rows: Vec<Vec<Expr>>,
        order: Vec<SortKey>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    SetOp {
        op: SetOpKind,
        all: bool,
        left: Box<Query>,
        right: Box<Query>,
        order: Vec<SortKey>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    With {
        ctes: Vec<CtePlan>,
        body: Box<Query>,
    },
    /// Recursive CTE: seed UNION [ALL] step, where the step reads `slot`.
    Recursive {
        slot: usize,
        seed: Box<Query>,
        step: Box<Query>,
        all: bool,
    },
    /// Data-modifying statement used as a query (RETURNING / CTE).
    Dml(Box<Dml>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConflictAction {
    Nothing,
    /// Assignments over [target row ++ excluded row], optional WHERE.
    Update {
        sets: Vec<(usize, Expr)>,
        filter: Option<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct OnConflict {
    /// Arbiter unique columns (None = any unique constraint).
    pub target: Option<Vec<usize>>,
    pub action: ConflictAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Dml {
    Insert {
        table: u32,
        /// Target column per source column.
        cols: Vec<usize>,
        source: Query,
        /// Default expression per table column.
        defaults: Vec<Option<Expr>>,
        on_conflict: Option<OnConflict>,
        returning: Vec<Expr>,
        overriding_system: bool,
    },
    Update {
        table: u32,
        from: Option<From>,
        filter: Option<Expr>,
        sets: Vec<(usize, Expr)>,
        defaults: Vec<Option<Expr>>,
        returning: Vec<Expr>,
    },
    Delete {
        table: u32,
        using: Option<From>,
        filter: Option<Expr>,
        returning: Vec<Expr>,
    },
}

/// A planned statement ready to run with bound parameters.
#[derive(Clone, Debug)]
pub struct Planned {
    pub query: Query,
    pub cols: Vec<OutCol>,
    /// The command tag's verb: SELECT, INSERT, UPDATE, DELETE.
    pub tag: &'static str,
    /// Number of CTE slots the executor needs.
    pub cte_slots: usize,
    /// Whether the statement returns rows (SELECT or RETURNING).
    pub returns_rows: bool,
}
