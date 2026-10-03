use crate::mysql::catalog::{Column, ColumnType};
use crate::mysql::error::MySqlError;
use crate::mysql::plan::{AggFunc, ArithOp, CmpOp, Expr, JoinOp, Plan, contains_agg};
use crate::mysql::types::Value;
use sqlparser::ast::{
    Assignment, BinaryOperator, ColumnDef, DataType, Expr as AstExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, OrderByKind, OrderBySort, Query, SelectItem, SetExpr, Statement, TableConstraint,
    TableFactor, TableWithJoins, UnaryOperator, Value as AstValue,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Binder {
    pub current_db: Option<String>,
    pub prepared_types: HashMap<String, Vec<Value>>,
    /// Assigns each `?` placeholder encountered while binding an
    /// expression its positional index (0, 1, 2, ...), in left-to-right
    /// order — matching how `COM_STMT_EXECUTE` lays out bound parameter
    /// values on the wire.
    param_counter: usize,
}

impl Binder {
    pub fn new(current_db: Option<String>) -> Self {
        Self { current_db, prepared_types: HashMap::new(), param_counter: 0 }
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
            Statement::CreateTable(create_table) => self.bind_create_table(
                create_table.name,
                create_table.columns,
                create_table.constraints,
            ),
            Statement::CreateDatabase { db_name, if_not_exists, .. } => {
                let name = db_name
                    .0
                    .iter()
                    .filter_map(|p| match p {
                        sqlparser::ast::ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(".");
                Ok(Plan::CreateDatabase { name, if_not_exists })
            }
            Statement::CreateIndex(ci) => {
                let (db, table) = self.resolve_table_name(&ci.table_name)?;
                let columns = ci
                    .columns
                    .into_iter()
                    .filter_map(|c| match c.column.expr {
                        AstExpr::Identifier(id) => Some(id.value),
                        _ => None,
                    })
                    .collect();
                Ok(Plan::CreateIndex { db, table, columns, if_not_exists: ci.if_not_exists })
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
        constraints: Vec<TableConstraint>,
    ) -> Result<Plan, MySqlError> {
        let (db, table) = self.resolve_table_name(&name)?;
        let mut cols = Vec::new();

        for col_def in columns {
            let col_name = col_def.name.value.clone();
            let col_type = match &col_def.data_type {
                DataType::Int(_)
                | DataType::Integer(_)
                | DataType::IntUnsigned(_)
                | DataType::IntegerUnsigned(_)
                | DataType::TinyInt(_)
                | DataType::TinyIntUnsigned(_)
                | DataType::UTinyInt
                | DataType::SmallInt(_)
                | DataType::SmallIntUnsigned(_)
                | DataType::MediumInt(_)
                | DataType::MediumIntUnsigned(_) => ColumnType::Int,
                // No dedicated unsigned/width-limited storage type -- values
                // are stored as a plain i64 either way (see "simple over
                // performant" in the project's own philosophy), so UNSIGNED
                // and the various display-width variants are accepted but
                // not distinguished from their signed/plain counterparts.
                DataType::BigInt(_) | DataType::BigIntUnsigned(_) => ColumnType::BigInt,
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
                DataType::Text | DataType::TinyText | DataType::MediumText | DataType::LongText => {
                    ColumnType::Text
                }
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
            let mut default = None;

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
                    // A default that's a literal (the overwhelmingly common
                    // case: `DEFAULT 0`, `DEFAULT ''`, `DEFAULT NULL`,
                    // including WordPress's own schema's `DEFAULT '0'` on
                    // NOT NULL columns like comment_count) binds to a
                    // `Const`, which is stored directly. A non-constant
                    // default (`DEFAULT CURRENT_TIMESTAMP`, an expression)
                    // isn't evaluated here -- falls back to no default,
                    // same as before this existed at all.
                    sqlparser::ast::ColumnOption::Default(expr) => {
                        if let Ok(Expr::Const(v)) = self.bind_expr(expr.clone()) {
                            default = Some(v);
                        }
                    }
                    _ => {}
                }
            }

            cols.push(Column {
                name: col_name,
                ty: col_type,
                not_null,
                default,
                auto_increment,
                primary_key,
            });
        }

        // A table-level `PRIMARY KEY (...)` clause (WordPress-style schemas
        // always define it this way, never as a column option) was
        // previously discarded entirely rather than just its enforcement --
        // mark the referenced column(s) as the primary key so that metadata
        // isn't lost, even though (like column-level `UNIQUE`, already a
        // no-op above) uniqueness itself still isn't enforced anywhere in
        // this engine. Other table-level constraint kinds (`UNIQUE`,
        // `FOREIGN KEY`, `KEY`/`INDEX`, `FULLTEXT`/`SPATIAL`) are accepted
        // but not tracked, for the same reason.
        for constraint in &constraints {
            if let TableConstraint::PrimaryKey(pk) = constraint {
                for idx_col in &pk.columns {
                    if let AstExpr::Identifier(ident) = &idx_col.column.expr {
                        for col in cols.iter_mut() {
                            if col.name == ident.value {
                                col.primary_key = true;
                                col.not_null = true;
                            }
                        }
                    }
                }
            }
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
        let order_by = query.order_by;
        let limit_clause = query.limit_clause;
        match *query.body {
            SetExpr::Select(select) => {
                let mut source =
                    if select.from.is_empty() { Plan::Dummy } else { self.bind_from(select.from)? };

                if let Some(selection) = select.selection {
                    let pred = self.bind_expr(selection)?;
                    source = Plan::Filter { source: Box::new(source), predicate: pred };
                }

                // `ORDER BY`/`LIMIT`/`OFFSET` sit between the row source and
                // the projection (see `Plan::Sort`'s own doc comment for
                // why), so this has to happen here, before `exprs`/`names`
                // are built below.
                let keys = match order_by {
                    Some(sqlparser::ast::OrderBy {
                        kind: OrderByKind::Expressions(exprs), ..
                    }) => exprs
                        .into_iter()
                        .map(|e| {
                            let asc = !matches!(e.options.sort, Some(OrderBySort::Desc));
                            Ok((self.bind_expr(e.expr)?, asc))
                        })
                        .collect::<Result<Vec<_>, MySqlError>>()?,
                    Some(sqlparser::ast::OrderBy { kind: OrderByKind::All(_), .. }) => {
                        return Err(MySqlError::unsupported("ORDER BY ALL"));
                    }
                    None => Vec::new(),
                };
                let (limit, offset) = match limit_clause {
                    Some(LimitClause::LimitOffset { limit, offset, limit_by }) => {
                        if !limit_by.is_empty() {
                            return Err(MySqlError::unsupported("LIMIT BY"));
                        }
                        let limit = limit.as_ref().map(expr_to_u64).transpose()?;
                        let offset = offset.as_ref().map(|o| expr_to_u64(&o.value)).transpose()?;
                        (limit, offset)
                    }
                    Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                        (Some(expr_to_u64(&limit)?), Some(expr_to_u64(&offset)?))
                    }
                    None => (None, None),
                };
                let calc_found_rows =
                    select.select_modifiers.as_ref().is_some_and(|m| m.sql_calc_found_rows);
                if !keys.is_empty() || limit.is_some() || offset.is_some() || calc_found_rows {
                    source = Plan::Sort {
                        source: Box::new(source),
                        keys,
                        limit,
                        offset,
                        calc_found_rows,
                    };
                }

                let group_exprs: Vec<Expr> = match select.group_by {
                    GroupByExpr::Expressions(exprs, _) => {
                        exprs.into_iter().map(|e| self.bind_expr(e)).collect::<Result<_, _>>()?
                    }
                    GroupByExpr::All(_) => {
                        return Err(MySqlError::unsupported("GROUP BY ALL"));
                    }
                };

                let mut exprs = Vec::new();
                let mut names = Vec::new();
                for item in select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            // Real MySQL labels an unaliased plain column
                            // reference with the column's own name (this is
                            // the overwhelmingly common case real apps hit:
                            // `SELECT option_name, option_value FROM
                            // wp_options`), and labels anything else with
                            // the expression's own source text -- not
                            // replicated here (`"?"` stays as a placeholder
                            // for those), since it needs the original SQL
                            // slice, not just the parsed AST.
                            let name = match &expr {
                                AstExpr::Identifier(ident) => ident.value.clone(),
                                AstExpr::CompoundIdentifier(idents) => idents
                                    .last()
                                    .map(|i| i.value.clone())
                                    .unwrap_or_else(|| "?".to_string()),
                                _ => "?".to_string(),
                            };
                            exprs.push(self.bind_expr(expr)?);
                            names.push(name);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            exprs.push(self.bind_expr(expr)?);
                            names.push(alias.value);
                        }
                        // `SELECT *` and `SELECT table.*` -- the qualifier
                        // (if any) is dropped the same way a qualified
                        // column reference already is: this engine only
                        // ever binds one table into scope per query.
                        // Expanded to the real per-column values (and, in
                        // `plan::column_names`, the real per-column names)
                        // at execution time, not here -- the binder has no
                        // catalog access to look the table's columns up.
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                            exprs.push(Expr::Wildcard);
                            names.push("*".to_string());
                        }
                        _ => return Err(MySqlError::unsupported("select item")),
                    }
                }

                let having = select.having.map(|h| self.bind_expr(h)).transpose()?;

                if !group_exprs.is_empty() || exprs.iter().any(contains_agg) || having.is_some() {
                    Ok(Plan::Aggregate {
                        source: Box::new(source),
                        group_exprs,
                        exprs,
                        names,
                        having,
                    })
                } else {
                    Ok(Plan::Project { source: Box::new(source), exprs, names })
                }
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
            // Found via testing before a public release: a bare `JOIN`
            // (no `INNER`/`LEFT`/... keyword -- sqlparser's own generic
            // `JoinOperator::Join`, not `::Inner`) wasn't matched at all,
            // nor was a bare `LEFT JOIN` without the optional `OUTER`
            // keyword (`::Left`, a variant distinct from `::LeftOuter`) --
            // both are at least as common as the forms that were already
            // handled, if not more so.
            let (op, swap_sides) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinOp::Inner(self.bind_join_constraint(constraint)?), false)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinOp::Left(self.bind_join_constraint(constraint)?), false)
                }
                // `A RIGHT JOIN B ON cond` has no direct equivalent in
                // this engine's `JoinOp` (`Inner`/`Left`/`Cross` only) --
                // expressed instead as `B LEFT JOIN A ON cond`, same
                // condition, sides swapped. Values stay correct either
                // way since every column reference resolves by name, not
                // position; the one real difference is `SELECT *`'s
                // column order, which comes out as the right table's
                // columns before the left's rather than matching real
                // MySQL's left-then-right order -- a cosmetic gap, not a
                // correctness one.
                JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                    (JoinOp::Left(self.bind_join_constraint(constraint)?), true)
                }
                JoinOperator::CrossJoin(_) => (JoinOp::Cross, false),
                _ => return Err(MySqlError::unsupported("join operator")),
            };
            plan = if swap_sides {
                Plan::Join { left: Box::new(right), right: Box::new(plan), op }
            } else {
                Plan::Join { left: Box::new(plan), right: Box::new(right), op }
            };
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
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Placeholder(_),
                ..
            }) => {
                let idx = self.param_counter;
                self.param_counter += 1;
                Ok(Expr::Param(idx))
            }
            AstExpr::Function(func) => self.bind_function(func),
            AstExpr::Identifier(ident) => {
                if ident.value.starts_with("@@") {
                    Ok(Expr::SysVar(ident.value[2..].to_string()))
                } else {
                    Ok(Expr::ColName(ident.value.clone()))
                }
            }
            // A qualified column reference (`table.col`, or even
            // `db.table.col`). Found via testing before a public release:
            // this used to drop every qualifier and keep only the final
            // part, on the claim that "joins aren't bound through this
            // path" -- false, a join's own ON condition (and any SELECT
            // list/WHERE referencing both sides) binds through exactly
            // this arm. Dropping the qualifier meant `cust.id` and
            // `orders.id` (any two joined tables sharing a column name --
            // extremely common, e.g. every table having its own `id`)
            // silently resolved to the *same* physical column, whichever
            // table's happened to come first, regardless of which one was
            // actually named: joining on `cust.id = orders.customer_id`
            // and then selecting `orders.id` would silently return
            // `cust.id`'s value instead. Now the full dotted path is kept,
            // and `Executor::eval_expr`'s `ColName` lookup (see its own
            // comment) tries an exact qualified match first before
            // falling back to a bare-name match -- so a qualified
            // reference against a join's merged, qualified column list
            // resolves to the real table it names, and a plain unqualified
            // reference still works exactly as before for the ordinary
            // single-table case.
            AstExpr::CompoundIdentifier(idents) => {
                if idents.is_empty() {
                    return Err(MySqlError::unsupported("empty compound identifier"));
                }
                let dotted = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                Ok(Expr::ColName(dotted))
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
                    BinaryOperator::Modulo => {
                        Ok(Expr::Arith { op: ArithOp::Mod, left: Box::new(l), right: Box::new(r) })
                    }
                    _ => Err(MySqlError::unsupported("binary operator")),
                }
            }
            AstExpr::InList { expr, list, negated } => {
                let bound_expr = Box::new(self.bind_expr(*expr)?);
                let bound_list =
                    list.into_iter().map(|e| self.bind_expr(e)).collect::<Result<_, _>>()?;
                Ok(Expr::InList { expr: bound_expr, list: bound_list, negated })
            }
            AstExpr::Nested(inner) => self.bind_expr(*inner),
            AstExpr::UnaryOp { op: UnaryOperator::Not, expr } => {
                Ok(Expr::Not(Box::new(self.bind_expr(*expr)?)))
            }
            AstExpr::UnaryOp { op: UnaryOperator::Minus, expr } => {
                // No literal negative-number AST node in sqlparser for this
                // dialect's integer literals -- `-5` arrives as a unary
                // minus over `5`. Expressed as `0 - expr` rather than a new
                // `Expr` variant, since every numeric `Expr` already
                // supports `Arith`.
                let e = self.bind_expr(*expr)?;
                Ok(Expr::Arith {
                    op: ArithOp::Sub,
                    left: Box::new(Expr::Const(Value::Int(0))),
                    right: Box::new(e),
                })
            }
            AstExpr::UnaryOp { op: UnaryOperator::Plus, expr } => self.bind_expr(*expr),
            AstExpr::IsNull(e) => Ok(Expr::IsNull(Box::new(self.bind_expr(*e)?), false)),
            AstExpr::IsNotNull(e) => Ok(Expr::IsNull(Box::new(self.bind_expr(*e)?), true)),
            AstExpr::Between { expr, negated, low, high } => {
                let x = self.bind_expr(*expr)?;
                let lo = self.bind_expr(*low)?;
                let hi = self.bind_expr(*high)?;
                let ge =
                    Expr::Compare { op: CmpOp::Ge, left: Box::new(x.clone()), right: Box::new(lo) };
                let le = Expr::Compare { op: CmpOp::Le, left: Box::new(x), right: Box::new(hi) };
                let both = Expr::And(vec![ge, le]);
                Ok(if negated { Expr::Not(Box::new(both)) } else { both })
            }
            AstExpr::Like { negated, expr, pattern, escape_char, any } => {
                if any {
                    return Err(MySqlError::unsupported("LIKE ANY"));
                }
                let e = self.bind_expr(*expr)?;
                let p = self.bind_expr(*pattern)?;
                let esc = match escape_char {
                    Some(c) => self.bind_expr(*c)?,
                    // MySQL's own default when no `ESCAPE` clause is given.
                    None => Expr::Const(Value::Text("\\".to_string())),
                };
                Ok(Expr::Like {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                    escape: Box::new(esc),
                    negated,
                })
            }
            AstExpr::Substring { expr, substring_from, substring_for, .. } => {
                // sqlparser parses any `SUBSTRING(...)` call -- both the
                // comma form this engine's executor expects and the
                // `FROM ... FOR ...` SQL-standard form -- into this
                // dedicated AST node, never a plain `AstExpr::Function`.
                let mut args = vec![self.bind_expr(*expr)?];
                if let Some(f) = substring_from {
                    args.push(self.bind_expr(*f)?);
                }
                if let Some(l) = substring_for {
                    args.push(self.bind_expr(*l)?);
                }
                Ok(Expr::Call { name: "SUBSTRING".to_string(), args })
            }
            AstExpr::Case { operand, conditions, else_result, .. } => {
                let bound_operand = match operand {
                    Some(o) => Some(self.bind_expr(*o)?),
                    None => None,
                };
                let mut bound_conditions = Vec::with_capacity(conditions.len());
                for w in conditions {
                    let cond = self.bind_expr(w.condition)?;
                    let result = self.bind_expr(w.result)?;
                    // A simple `CASE x WHEN v THEN r` compares `x = v`; a
                    // searched `CASE WHEN cond THEN r` uses `cond` as-is.
                    let cond = match &bound_operand {
                        Some(op) => Expr::Compare {
                            op: CmpOp::Eq,
                            left: Box::new(op.clone()),
                            right: Box::new(cond),
                        },
                        None => cond,
                    };
                    bound_conditions.push((cond, result));
                }
                let bound_else = match else_result {
                    Some(e) => Some(Box::new(self.bind_expr(*e)?)),
                    None => None,
                };
                Ok(Expr::Case { conditions: bound_conditions, else_result: bound_else })
            }
            _ => Err(MySqlError::unsupported("expr")),
        }
    }

    /// Binds `COUNT`/`COUNT(*)`/`SUM`/`AVG`/`MIN`/`MAX` calls to
    /// `Expr::Agg`. Any other function name is unsupported — this engine
    /// has no scalar function library yet.
    fn bind_function(&mut self, func: Function) -> Result<Expr, MySqlError> {
        let name = func
            .name
            .0
            .iter()
            .filter_map(|p| match p {
                sqlparser::ast::ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(".");
        let upper = name.to_uppercase();

        let args = match func.args {
            FunctionArguments::List(list) => list.args,
            FunctionArguments::None => vec![],
            FunctionArguments::Subquery(_) => {
                return Err(MySqlError::unsupported("function with subquery argument"));
            }
        };

        match upper.as_str() {
            "COUNT" => {
                if args.len() == 1
                    && matches!(&args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                {
                    Ok(Expr::Agg { func: AggFunc::CountStar, arg: None })
                } else if args.len() == 1 {
                    let arg = self.bind_function_arg(&args[0])?;
                    Ok(Expr::Agg { func: AggFunc::Count, arg: Some(Box::new(arg)) })
                } else {
                    Err(MySqlError::unsupported("COUNT argument list"))
                }
            }
            "SUM" | "AVG" | "MIN" | "MAX" => {
                if args.len() != 1 {
                    return Err(MySqlError::unsupported(&format!("{upper} argument list")));
                }
                let arg = self.bind_function_arg(&args[0])?;
                let agg_func = match upper.as_str() {
                    "SUM" => AggFunc::Sum,
                    "AVG" => AggFunc::Avg,
                    "MIN" => AggFunc::Min,
                    "MAX" => AggFunc::Max,
                    _ => unreachable!(),
                };
                Ok(Expr::Agg { func: agg_func, arg: Some(Box::new(arg)) })
            }
            "FOUND_ROWS" => {
                if !args.is_empty() {
                    return Err(MySqlError::unsupported("FOUND_ROWS argument list"));
                }
                Ok(Expr::FoundRows)
            }
            // A small, fixed scalar-function library (no general catalog)
            // -- see `Executor::eval_call` for what each one actually
            // computes. Argument-count validation happens there too, not
            // here, so it stays next to the logic it's validating.
            "CONCAT" | "UPPER" | "LOWER" | "LENGTH" | "SUBSTRING" | "SUBSTR" | "COALESCE"
            | "IFNULL" => {
                let bound_args =
                    args.iter().map(|a| self.bind_function_arg(a)).collect::<Result<_, _>>()?;
                Ok(Expr::Call { name: upper, args: bound_args })
            }
            _ => Err(MySqlError::unsupported(&format!("function {name}"))),
        }
    }

    fn bind_function_arg(&mut self, arg: &FunctionArg) -> Result<Expr, MySqlError> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => self.bind_expr(e.clone()),
            _ => Err(MySqlError::unsupported("function argument form")),
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

/// Extracts a `LIMIT`/`OFFSET` value: real MySQL only accepts a plain
/// non-negative integer literal there (no expressions, no placeholders),
/// so this rejects anything else rather than trying to evaluate it.
fn expr_to_u64(expr: &AstExpr) -> Result<u64, MySqlError> {
    if let AstExpr::Value(sqlparser::ast::ValueWithSpan { value: AstValue::Number(s, _), .. }) =
        expr
        && let Ok(n) = s.parse::<u64>()
    {
        return Ok(n);
    }
    Err(MySqlError::unsupported("LIMIT/OFFSET value"))
}
