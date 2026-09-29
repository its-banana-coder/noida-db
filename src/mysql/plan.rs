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

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Const(Value),
    Param(usize),
    Col(usize),
    ColName(String),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Compare { op: CmpOp, left: Box<Expr>, right: Box<Expr> },
    Arith { op: ArithOp, left: Box<Expr>, right: Box<Expr> },
    Call { name: String, args: Vec<Expr> },
    SysVar(String),
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
    ShowColumns { db: String, table: String },
    ShowCreateTable { db: String, table: String },
    Use(String),
    Filter { source: Box<Plan>, predicate: Expr },
    Project { source: Box<Plan>, exprs: Vec<Expr>, names: Vec<String> },
    Scan { db: String, table: String },
    Join { left: Box<Plan>, right: Box<Plan>, op: JoinOp },
    CreateTable { db: String, table: String, columns: Vec<Column> },
    Insert { db: String, table: String, columns: Vec<String>, rows: Vec<Vec<Expr>> },
    Update { db: String, table: String, assignments: Vec<(String, Expr)>, selection: Option<Expr> },
    Delete { db: String, table: String, selection: Option<Expr> },
}
