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

#[derive(Clone, Debug, PartialEq)]
pub enum Plan {
    Dummy, // SELECT 1
    ShowDatabases,
    ShowTables(String),
    Use(String),
    Filter { source: Box<Plan>, predicate: Expr },
    Project { source: Box<Plan>, exprs: Vec<Expr>, names: Vec<String> },
    Scan { db: String, table: String },
    Join { left: Box<Plan>, right: Box<Plan>, op: JoinOp },
}
