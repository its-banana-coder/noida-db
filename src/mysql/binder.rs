use crate::mysql::catalog::{Column, ColumnType};
use crate::mysql::error::MySqlError;
use crate::mysql::plan::{ArithOp, CmpOp, Expr, JoinOp, Plan};
use crate::mysql::types::Value;
use sqlparser::ast::{
    Assignment, BinaryOperator, ColumnDef, DataType, Expr as AstExpr, JoinConstraint, JoinOperator,
    ObjectName, Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    Value as AstValue,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Binder {
    pub current_db: Option<String>,
    pub prepared_types: HashMap<String, Vec<Value>>,
}

impl Binder {
    pub fn new(current_db: Option<String>) -> Self {
        Self { current_db, prepared_types: HashMap::new() }
    }

    pub fn bind_statement(&mut self, stmt: Statement) -> Result<Plan, MySqlError> {
        match stmt {
            Statement::Query(query) => self.bind_query(*query),
            Statement::ShowDatabases { .. } => Ok(Plan::ShowDatabases),
            Statement::ShowTables { .. } => {
                let db = self.current_db.clone().unwrap_or_default();
                Ok(Plan::ShowTables(db))
            }
            Statement::ShowColumns { show_options, .. } => {
                let table_name_str = match show_options.show_in {
                    Some(sqlparser::ast::ShowStatementIn { parent_name: Some(name), .. }) => {
                        let parts: Vec<String> = name
                            .0
                            .iter()
                            .filter_map(|n| match n {
                                sqlparser::ast::ObjectNamePart::Identifier(id) => {
                                    Some(id.value.clone())
                                }
                                _ => None,
                            })
                            .collect();
                        parts.join(".")
                    }
                    _ => return Err(MySqlError::unsupported("SHOW COLUMNS without IN table")),
                };
                let obj = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new(table_name_str),
                )]);
                let (db, table) = self.resolve_table_name(&obj)?;
                Ok(Plan::ShowColumns { db, table })
            }
            Statement::ShowCreate { obj_type, obj_name } => {
                if let sqlparser::ast::ShowCreateObject::Table = obj_type {
                    let (db, table) = self.resolve_table_name(&obj_name)?;
                    Ok(Plan::ShowCreateTable { db, table })
                } else {
                    Err(MySqlError::unsupported("SHOW CREATE object type"))
                }
            }
            Statement::Use(use_db) => match use_db {
                sqlparser::ast::Use::Object(db_name) => {
                    let name = db_name
                        .0
                        .iter()
                        .map(|n| match n {
                            sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                            _ => "".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(".");
                    Ok(Plan::Use(name))
                }
                _ => Err(MySqlError::unsupported("USE statement format")),
            },
            Statement::CreateTable(create_table) => {
                self.bind_create_table(create_table.name, create_table.columns)
            }
            Statement::Insert(insert) => self.bind_insert(insert),
            Statement::Update(update) => {
                self.bind_update(update.table, update.assignments, update.selection)
            }
            Statement::Delete(delete) => {
                let from = match delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(f) => f,
                    sqlparser::ast::FromTable::WithoutKeyword(f) => f,
                };
                self.bind_delete(from, delete.selection)
            }
            _ => Err(MySqlError::unsupported("statement")),
        }
    }

    fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: Vec<ColumnDef>,
    ) -> Result<Plan, MySqlError> {
        let (db, table) = self.resolve_table_name(&name)?;
        let mut cols = Vec::new();

        for col_def in columns {
            let col_name = col_def.name.value.clone();
            let col_type = match &col_def.data_type {
                DataType::Int(_) | DataType::Integer(_) => ColumnType::Int,
                DataType::BigInt(_) => ColumnType::BigInt,
                DataType::Varchar(len) => {
                    let l = len
                        .as_ref()
                        .and_then(|e| match &e {
                            sqlparser::ast::CharacterLength::IntegerLength { length, .. } => {
                                Some(*length as usize)
                            }
                            _ => None,
                        })
                        .unwrap_or(255);
                    ColumnType::Varchar(l)
                }
                DataType::Text => ColumnType::Text,
                DataType::Float(_) => ColumnType::Float,
                DataType::Double(_) => ColumnType::Double,
                DataType::Decimal(exact) => {
                    let (p, s) = match exact {
                        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                            (*p as u8, *s as u8)
                        }
                        sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                        sqlparser::ast::ExactNumberInfo::None => (10, 0),
                    };
                    ColumnType::Decimal(p, s)
                }
                DataType::Date => ColumnType::Date,
                DataType::Datetime(_) => ColumnType::Datetime,
                DataType::Boolean => ColumnType::Boolean,
                _ => {
                    return Err(MySqlError::unsupported(&format!(
                        "data type {:?}",
                        col_def.data_type
                    )));
                }
            };

            let mut not_null = false;
            let mut auto_increment = false;
            let mut primary_key = false;

            for opt in &col_def.options {
                match &opt.option {
                    sqlparser::ast::ColumnOption::NotNull => not_null = true,
                    sqlparser::ast::ColumnOption::Unique(u) => {
                        if u.index_name.is_none() && u.index_type.is_none() {
                            // Unique
                        }
                    }
                    sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                        primary_key = true;
                        not_null = true;
                    }
                    sqlparser::ast::ColumnOption::DialectSpecific(tokens) => {
                        let text =
                            tokens.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(" ");
                        if text.eq_ignore_ascii_case("auto_increment") {
                            auto_increment = true;
                        }
                    }
                    _ => {}
                }
            }

            cols.push(Column {
                name: col_name,
                ty: col_type,
                not_null,
                default: None, // TODO
                auto_increment,
                primary_key,
            });
        }

        Ok(Plan::CreateTable { db, table, columns: cols })
    }

    fn bind_insert(&mut self, insert: sqlparser::ast::Insert) -> Result<Plan, MySqlError> {
        let table_name = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => name,
            _ => return Err(MySqlError::unsupported("insert table target")),
        };
        let (db, table) = self.resolve_table_name(table_name)?;
        let columns = insert
            .columns
            .iter()
            .filter_map(|name| {
                if name.0.len() != 1 {
                    None
                } else {
                    match &name.0[0] {
                        sqlparser::ast::ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                        _ => None,
                    }
                }
            })
            .collect();
        let mut rows = Vec::new();

        if let Some(source) = insert.source {
            if let SetExpr::Values(values) = *source.body {
                for row in values.rows {
                    let mut r = Vec::new();
                    for expr in row.content {
                        r.push(self.bind_expr(expr)?);
                    }
                    rows.push(r);
                }
            } else {
                return Err(MySqlError::unsupported("insert source"));
            }
        }

        Ok(Plan::Insert { db, table, columns, rows })
    }

    fn bind_update(
        &mut self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<AstExpr>,
    ) -> Result<Plan, MySqlError> {
        let (db, table_name) = match &table.relation {
            TableFactor::Table { name, .. } => self.resolve_table_name(name)?,
            _ => return Err(MySqlError::unsupported("update target")),
        };

        let mut out_assignments = Vec::new();
        for a in assignments {
            let col_name = match &a.target {
                sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                    if name.0.len() != 1 {
                        return Err(MySqlError::unsupported("update column name length"));
                    }
                    match &name.0[0] {
                        sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                        _ => return Err(MySqlError::unsupported("update column name format")),
                    }
                }
                _ => return Err(MySqlError::unsupported("update assignment target")),
            };
            let expr = self.bind_expr(a.value)?;
            out_assignments.push((col_name, expr));
        }

        let sel = match selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };

        Ok(Plan::Update { db, table: table_name, assignments: out_assignments, selection: sel })
    }

    fn bind_delete(
        &mut self,
        from: Vec<TableWithJoins>,
        selection: Option<AstExpr>,
    ) -> Result<Plan, MySqlError> {
        if from.len() != 1 {
            return Err(MySqlError::unsupported("delete multiple tables"));
        }
        let (db, table_name) = match &from[0].relation {
            TableFactor::Table { name, .. } => self.resolve_table_name(name)?,
            _ => return Err(MySqlError::unsupported("delete target")),
        };

        let sel = match selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };

        Ok(Plan::Delete { db, table: table_name, selection: sel })
    }

    fn bind_query(&mut self, query: Query) -> Result<Plan, MySqlError> {
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
                        _ => return Err(MySqlError::unsupported("select item")),
                    }
                }

                Ok(Plan::Project { source: Box::new(source), exprs, names })
            }
            _ => Err(MySqlError::unsupported("query body")),
        }
    }

    fn bind_from(&mut self, from: Vec<TableWithJoins>) -> Result<Plan, MySqlError> {
        if from.len() != 1 {
            return Err(MySqlError::unsupported("multiple from clauses"));
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
                _ => return Err(MySqlError::unsupported("join operator")),
            };
            plan = Plan::Join { left: Box::new(plan), right: Box::new(right), op };
        }

        Ok(plan)
    }

    fn bind_table_factor(&mut self, tf: &TableFactor) -> Result<Plan, MySqlError> {
        match tf {
            TableFactor::Table { name, .. } => {
                let (db, table) = self.resolve_table_name(name)?;
                Ok(Plan::Scan { db, table })
            }
            _ => Err(MySqlError::unsupported("table factor")),
        }
    }

    fn bind_join_constraint(&mut self, c: &JoinConstraint) -> Result<Expr, MySqlError> {
        match c {
            JoinConstraint::On(expr) => self.bind_expr(expr.clone()),
            _ => Err(MySqlError::unsupported("join constraint")),
        }
    }

    fn bind_expr(&mut self, expr: AstExpr) -> Result<Expr, MySqlError> {
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
                    Ok(Expr::ColName(ident.value.clone()))
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
                    _ => Err(MySqlError::unsupported("binary operator")),
                }
            }
            _ => Err(MySqlError::unsupported("expr")),
        }
    }

    fn resolve_table_name(&self, name: &ObjectName) -> Result<(String, String), MySqlError> {
        if name.0.len() == 1 {
            let db = self
                .current_db
                .clone()
                .ok_or_else(|| MySqlError::new(1046, "3D000", "No database selected"))?;
            Ok((
                db,
                match &name.0[0] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err(MySqlError::syntax_error("bad identifier")),
                },
            ))
        } else if name.0.len() == 2 {
            Ok((
                match &name.0[0] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err(MySqlError::syntax_error("bad identifier")),
                },
                match &name.0[1] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err(MySqlError::syntax_error("bad identifier")),
                },
            ))
        } else {
            Err(MySqlError::syntax_error("invalid table name"))
        }
    }
}
