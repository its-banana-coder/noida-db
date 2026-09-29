use crate::mysql::plan::{ArithOp, CmpOp, Expr, JoinOp, Plan};
use crate::mysql::types::Value;
use sqlparser::ast::{
    BinaryOperator, Expr as AstExpr, JoinConstraint, JoinOperator, ObjectName, Query, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Value as AstValue,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Binder {
    pub current_db: Option<String>,
    pub prepared_types: HashMap<String, Vec<Value>>,
}

impl Binder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind_statement(&mut self, stmt: Statement) -> Result<Plan, String> {
        match stmt {
            Statement::Query(query) => self.bind_query(*query),
            Statement::ShowDatabases { .. } => Ok(Plan::ShowDatabases),
            Statement::ShowTables { .. } => {
                let db = self.current_db.clone().unwrap_or_default();
                Ok(Plan::ShowTables(db))
            }
            Statement::Use(db_name) => Ok(Plan::Use(db_name.to_string())),
            _ => Err("unsupported statement".to_string()),
        }
    }

    fn bind_query(&mut self, query: Query) -> Result<Plan, String> {
        match *query.body {
            SetExpr::Select(select) => {
                let mut source =
                    if select.from.is_empty() { Plan::Dummy } else { self.bind_from(select.from)? };

                if let Some(selection) = select.selection {
                    let pred = self.bind_expr(selection)?;
                    source = Plan::Filter { source: Box::new(source), predicate: pred };
                }

                let mut exprs = Vec::new();
                let mut names = Vec::new();
                for item in select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            exprs.push(self.bind_expr(expr)?);
                            names.push("?".to_string());
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            exprs.push(self.bind_expr(expr)?);
                            names.push(alias.value);
                        }
                        _ => return Err("unsupported select item".to_string()),
                    }
                }

                Ok(Plan::Project { source: Box::new(source), exprs, names })
            }
            _ => Err("unsupported query body".to_string()),
        }
    }

    fn bind_from(&mut self, from: Vec<TableWithJoins>) -> Result<Plan, String> {
        if from.len() != 1 {
            return Err("multiple from clauses not supported".to_string());
        }

        let twj = &from[0];
        let mut plan = self.bind_table_factor(&twj.relation)?;

        for join in &twj.joins {
            let right = self.bind_table_factor(&join.relation)?;
            let op = match &join.join_operator {
                JoinOperator::Inner(constraint) => {
                    JoinOp::Inner(self.bind_join_constraint(constraint)?)
                }
                JoinOperator::LeftOuter(constraint) => {
                    JoinOp::Left(self.bind_join_constraint(constraint)?)
                }
                JoinOperator::CrossJoin(_) => JoinOp::Cross,
                _ => return Err("unsupported join operator".to_string()),
            };
            plan = Plan::Join { left: Box::new(plan), right: Box::new(right), op };
        }

        Ok(plan)
    }

    fn bind_table_factor(&mut self, tf: &TableFactor) -> Result<Plan, String> {
        match tf {
            TableFactor::Table { name, .. } => {
                let (db, table) = self.resolve_table_name(name)?;
                Ok(Plan::Scan { db, table })
            }
            _ => Err("unsupported table factor".to_string()),
        }
    }

    fn bind_join_constraint(&mut self, c: &JoinConstraint) -> Result<Expr, String> {
        match c {
            JoinConstraint::On(expr) => self.bind_expr(expr.clone()),
            _ => Err("unsupported join constraint".to_string()),
        }
    }

    fn bind_expr(&mut self, expr: AstExpr) -> Result<Expr, String> {
        match expr {
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Number(s, _), ..
            }) => {
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Expr::Const(Value::Int(i)))
                } else {
                    Ok(Expr::Const(Value::Text(s)))
                }
            }
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::SingleQuotedString(s),
                ..
            }) => Ok(Expr::Const(Value::Text(s))),
            AstExpr::Value(sqlparser::ast::ValueWithSpan { value: AstValue::Null, .. }) => {
                Ok(Expr::Const(Value::Null))
            }
            AstExpr::Identifier(ident) => {
                if ident.value.starts_with("@@") {
                    Ok(Expr::SysVar(ident.value[2..].to_string()))
                } else {
                    Ok(Expr::Col(0))
                }
            }
            AstExpr::BinaryOp { left, op, right } => {
                let l = self.bind_expr(*left)?;
                let r = self.bind_expr(*right)?;
                match op {
                    BinaryOperator::Eq => {
                        Ok(Expr::Compare { op: CmpOp::Eq, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::NotEq => {
                        Ok(Expr::Compare { op: CmpOp::Ne, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Lt => {
                        Ok(Expr::Compare { op: CmpOp::Lt, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::LtEq => {
                        Ok(Expr::Compare { op: CmpOp::Le, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Gt => {
                        Ok(Expr::Compare { op: CmpOp::Gt, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::GtEq => {
                        Ok(Expr::Compare { op: CmpOp::Ge, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::And => Ok(Expr::And(vec![l, r])),
                    BinaryOperator::Or => Ok(Expr::Or(vec![l, r])),
                    BinaryOperator::Plus => {
                        Ok(Expr::Arith { op: ArithOp::Add, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Minus => {
                        Ok(Expr::Arith { op: ArithOp::Sub, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Multiply => {
                        Ok(Expr::Arith { op: ArithOp::Mul, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Divide => {
                        Ok(Expr::Arith { op: ArithOp::Div, left: Box::new(l), right: Box::new(r) })
                    }
                    _ => Err("unsupported binary operator".to_string()),
                }
            }
            _ => Err("unsupported expr".to_string()),
        }
    }

    fn resolve_table_name(&self, name: &ObjectName) -> Result<(String, String), String> {
        if name.0.len() == 1 {
            let db = self.current_db.clone().ok_or("no database selected")?;
            Ok((
                db,
                match &name.0[0] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err("bad identifier".to_string()),
                },
            ))
        } else if name.0.len() == 2 {
            Ok((
                match &name.0[0] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err("bad identifier".to_string()),
                },
                match &name.0[1] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err("bad identifier".to_string()),
                },
            ))
        } else {
            Err("invalid table name".to_string())
        }
    }
}
