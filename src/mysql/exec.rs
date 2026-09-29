use crate::mysql::catalog::{DbState, Table};
use crate::mysql::error::MySqlError;
use crate::mysql::plan::{ArithOp, CmpOp, Expr, JoinOp, Plan};
use crate::mysql::types::Value;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

pub struct Executor {
    pub db: Arc<Mutex<DbState>>,
    pub current_db: Option<String>,
}

impl Executor {
    pub fn new(db: Arc<Mutex<DbState>>, current_db: Option<String>) -> Self {
        Self { db, current_db }
    }

    /// Finds the `Table` a plan's rows ultimately come from, for resolving
    /// `Expr::ColName` by name in `Filter`/`Project`. Looks through any
    /// chain of single-source wrapper nodes (`Filter`, `Project`) down to
    /// the underlying `Scan` — a bare `match` on `Plan::Scan` alone missed
    /// this for anything with a `WHERE` clause, since that wraps the scan
    /// in `Plan::Filter` first, silently resolving every column reference
    /// to `Value::Null` instead of erroring or working.
    fn resolve_table_context(&self, plan: &Plan) -> Result<Option<Table>, MySqlError> {
        match plan {
            Plan::Scan { db, table } => {
                let state = self.db.lock().unwrap();
                let schema = state.schemas.get(db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t = schema.tables.get(table).ok_or_else(|| MySqlError::unknown_table(table))?;
                Ok(Some((**t).clone()))
            }
            Plan::Filter { source, .. } => self.resolve_table_context(source),
            Plan::Project { source, .. } => self.resolve_table_context(source),
            _ => Ok(None),
        }
    }

    pub fn execute_plan(&mut self, plan: Plan) -> Result<Vec<Vec<Value>>, MySqlError> {
        match plan {
            Plan::Dummy => Ok(vec![vec![]]),
            Plan::ShowDatabases => Ok(vec![
                vec![Value::Text("information_schema".to_string())],
                vec![Value::Text("mysql".to_string())],
                vec![Value::Text("performance_schema".to_string())],
                vec![Value::Text("sys".to_string())],
                vec![Value::Text("test".to_string())],
            ]),
            Plan::Use(db) => {
                let state = self.db.lock().unwrap();
                if !state.schemas.contains_key(&db) {
                    return Err(MySqlError::new(
                        1049,
                        "42000",
                        format!("Unknown database '{}'", db),
                    ));
                }
                self.current_db = Some(db);
                Ok(vec![])
            }
            Plan::ShowTables(db) => {
                let state = self.db.lock().unwrap();
                let schema = state.schemas.get(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let mut rows = Vec::new();
                for table_name in schema.tables.keys() {
                    rows.push(vec![Value::Text(table_name.clone())]);
                }
                Ok(rows)
            }
            Plan::ShowColumns { db, table } => {
                let state = self.db.lock().unwrap();
                let schema = state.schemas.get(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t =
                    schema.tables.get(&table).ok_or_else(|| MySqlError::unknown_table(&table))?;
                let mut rows = Vec::new();
                for col in &t.columns {
                    rows.push(vec![
                        Value::Text(col.name.clone()),
                        Value::Text(format!("{:?}", col.ty)),
                        Value::Text(if col.not_null {
                            "NO".to_string()
                        } else {
                            "YES".to_string()
                        }),
                        Value::Text(if col.primary_key {
                            "PRI".to_string()
                        } else {
                            "".to_string()
                        }),
                        col.default.clone().unwrap_or(Value::Null),
                        Value::Text(if col.auto_increment {
                            "auto_increment".to_string()
                        } else {
                            "".to_string()
                        }),
                    ]);
                }
                Ok(rows)
            }
            Plan::ShowCreateTable { db, table } => {
                let state = self.db.lock().unwrap();
                let schema = state.schemas.get(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t =
                    schema.tables.get(&table).ok_or_else(|| MySqlError::unknown_table(&table))?;

                let mut sql = format!("CREATE TABLE `{}` (\n", t.name);
                let mut cols_sql = Vec::new();
                for col in &t.columns {
                    let mut col_sql = format!("  `{}` {:?}", col.name, col.ty);
                    if col.not_null {
                        col_sql.push_str(" NOT NULL");
                    }
                    if col.auto_increment {
                        col_sql.push_str(" AUTO_INCREMENT");
                    }
                    if col.primary_key {
                        col_sql.push_str(" PRIMARY KEY");
                    }
                    cols_sql.push(col_sql);
                }
                sql.push_str(&cols_sql.join(",\n"));
                sql.push_str("\n) ENGINE=InnoDB");

                Ok(vec![vec![Value::Text(t.name.clone()), Value::Text(sql)]])
            }
            Plan::CreateTable { db, table, columns } => {
                let mut state = self.db.lock().unwrap();
                let schema = state.schemas.get_mut(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                if schema.tables.contains_key(&table) {
                    return Err(MySqlError::new(
                        1050,
                        "42S01",
                        format!("Table '{}' already exists", table),
                    ));
                }
                schema.tables.insert(table.clone(), Arc::new(Table::new(table, columns)));
                Ok(vec![])
            }
            Plan::Insert { db, table, columns, rows } => {
                let mut state = self.db.lock().unwrap();
                let schema = state.schemas.get_mut(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t = schema
                    .tables
                    .get_mut(&table)
                    .ok_or_else(|| MySqlError::unknown_table(&table))?;
                let t_mut = Arc::make_mut(t);

                for row_exprs in rows {
                    let mut new_row = vec![Value::Null; t_mut.columns.len()];

                    let mut col_indices = Vec::new();
                    if columns.is_empty() {
                        for i in 0..t_mut.columns.len() {
                            col_indices.push(i);
                        }
                    } else {
                        for col_name in &columns {
                            let idx = t_mut
                                .columns
                                .iter()
                                .position(|c| c.name == *col_name)
                                .ok_or_else(|| MySqlError::unknown_column(col_name))?;
                            col_indices.push(idx);
                        }
                    }

                    if row_exprs.len() != col_indices.len() {
                        return Err(MySqlError::new(
                            1136,
                            "21S01",
                            "Column count doesn't match value count at row 1",
                        ));
                    }

                    for (i, expr) in row_exprs.iter().enumerate() {
                        let val = self.eval_expr(expr, &[], None)?;
                        new_row[col_indices[i]] = val;
                    }

                    for (i, col) in t_mut.columns.iter().enumerate() {
                        if new_row[i].is_null() {
                            if col.auto_increment {
                                new_row[i] = Value::Int(t_mut.next_auto_increment);
                                t_mut.next_auto_increment += 1;
                            } else if let Some(def) = &col.default {
                                new_row[i] = def.clone();
                            } else if col.not_null {
                                return Err(MySqlError::new(
                                    1364,
                                    "HY000",
                                    format!("Field '{}' doesn't have a default value", col.name),
                                ));
                            }
                        }
                    }

                    t_mut.rows.push(new_row);
                }

                Ok(vec![])
            }
            Plan::Update { db, table, assignments, selection } => {
                let mut state = self.db.lock().unwrap();
                let schema = state.schemas.get_mut(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t = schema
                    .tables
                    .get_mut(&table)
                    .ok_or_else(|| MySqlError::unknown_table(&table))?;
                let t_mut = Arc::make_mut(t);

                for i in 0..t_mut.rows.len() {
                    let mut matches = true;
                    if let Some(sel) = &selection {
                        let val = self.eval_expr(sel, &t_mut.rows[i], Some(&*t_mut))?;
                        if val.is_null() || val == Value::Int(0) {
                            matches = false;
                        }
                    }

                    if matches {
                        for (col_name, expr) in &assignments {
                            let idx = t_mut
                                .columns
                                .iter()
                                .position(|c| c.name == *col_name)
                                .ok_or_else(|| MySqlError::unknown_column(col_name))?;
                            let val = self.eval_expr(expr, &t_mut.rows[i], Some(&*t_mut))?;
                            t_mut.rows[i][idx] = val;
                        }
                    }
                }

                Ok(vec![])
            }
            Plan::Delete { db, table, selection } => {
                let mut state = self.db.lock().unwrap();
                let schema = state.schemas.get_mut(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t = schema
                    .tables
                    .get_mut(&table)
                    .ok_or_else(|| MySqlError::unknown_table(&table))?;
                let t_mut = Arc::make_mut(t);

                let mut new_rows = Vec::new();
                for row in &t_mut.rows {
                    let mut matches = true;
                    if let Some(sel) = &selection {
                        let val = self.eval_expr(sel, row, Some(&*t_mut))?;
                        if val.is_null() || val == Value::Int(0) {
                            matches = false;
                        }
                    }

                    if !matches {
                        new_rows.push(row.clone());
                    }
                }
                t_mut.rows = new_rows;

                Ok(vec![])
            }
            Plan::Scan { db, table } => {
                let state = self.db.lock().unwrap();
                let schema = state.schemas.get(&db).ok_or_else(|| {
                    MySqlError::new(1049, "42000", format!("Unknown database '{}'", db))
                })?;
                let t =
                    schema.tables.get(&table).ok_or_else(|| MySqlError::unknown_table(&table))?;
                Ok(t.rows.clone())
            }
            Plan::Filter { source, predicate } => {
                let table_context = self.resolve_table_context(&source)?;

                let rows = self.execute_plan(*source)?;
                let mut out_rows = Vec::new();
                for row in rows {
                    let val = self.eval_expr(&predicate, &row, table_context.as_ref())?;
                    if !val.is_null() && val != Value::Int(0) {
                        out_rows.push(row);
                    }
                }
                Ok(out_rows)
            }
            Plan::Project { source, exprs, .. } => {
                let table_context = self.resolve_table_context(&source)?;

                let rows = self.execute_plan(*source)?;
                let mut out_rows = Vec::new();
                for row in rows {
                    let mut out_row = Vec::new();
                    for expr in &exprs {
                        out_row.push(self.eval_expr(expr, &row, table_context.as_ref())?);
                    }
                    out_rows.push(out_row);
                }
                Ok(out_rows)
            }
            Plan::Join { left, right, op } => {
                let l_rows = self.execute_plan(*left)?;
                let r_rows = self.execute_plan(*right)?;
                let mut out_rows = Vec::new();

                match op {
                    JoinOp::Cross => {
                        for l in &l_rows {
                            for r in &r_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());
                                out_rows.push(row);
                            }
                        }
                    }
                    JoinOp::Inner(cond) => {
                        for l in &l_rows {
                            for r in &r_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());
                                let val = self.eval_expr(&cond, &row, None)?;
                                if !val.is_null() && val != Value::Int(0) {
                                    out_rows.push(row);
                                }
                            }
                        }
                    }
                    JoinOp::Left(cond) => {
                        let r_len = if r_rows.is_empty() { 0 } else { r_rows[0].len() };
                        for l in &l_rows {
                            let mut matched = false;
                            for r in &r_rows {
                                let mut row = l.clone();
                                row.extend(r.clone());
                                let val = self.eval_expr(&cond, &row, None)?;
                                if !val.is_null() && val != Value::Int(0) {
                                    out_rows.push(row);
                                    matched = true;
                                }
                            }
                            if !matched {
                                let mut row = l.clone();
                                row.extend(vec![Value::Null; r_len]);
                                out_rows.push(row);
                            }
                        }
                    }
                }
                Ok(out_rows)
            }
        }
    }

    fn eval_expr(
        &self,
        expr: &Expr,
        row: &[Value],
        table: Option<&Table>,
    ) -> Result<Value, MySqlError> {
        match expr {
            Expr::Const(v) => Ok(v.clone()),
            Expr::Param(_) => Err(MySqlError::unsupported("parameters in execution")),
            Expr::Col(i) => Ok(row.get(*i).cloned().unwrap_or(Value::Null)),
            Expr::ColName(name) => {
                if let Some(t) = table
                    && let Some(idx) = t.columns.iter().position(|c| c.name == *name)
                {
                    return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
                }
                Ok(Value::Null) // For simplicity, return Null if column not found or no table context
            }
            Expr::And(exprs) => {
                let mut res = Value::Int(1);
                for e in exprs {
                    let v = self.eval_expr(e, row, table)?;
                    if v.is_null() {
                        res = Value::Null;
                    } else if v == Value::Int(0) {
                        return Ok(Value::Int(0));
                    }
                }
                Ok(res)
            }
            Expr::Or(exprs) => {
                let mut res = Value::Int(0);
                for e in exprs {
                    let v = self.eval_expr(e, row, table)?;
                    if v == Value::Int(1) {
                        return Ok(Value::Int(1));
                    } else if v.is_null() {
                        res = Value::Null;
                    }
                }
                Ok(res)
            }
            Expr::Arith { op, left, right } => {
                let l = self.eval_expr(left, row, table)?;
                let r = self.eval_expr(right, row, table)?;
                eval_arith(*op, l, r)
            }
            Expr::Compare { op, left, right } => {
                let l = self.eval_expr(left, row, table)?;
                let r = self.eval_expr(right, row, table)?;
                eval_compare(*op, l, r)
            }
            Expr::SysVar(name) => {
                if name.eq_ignore_ascii_case("version") {
                    Ok(Value::Text("8.0.33".to_string()))
                } else if name.eq_ignore_ascii_case("version_comment") {
                    Ok(Value::Text("noida-db MySQL 8.0".to_string()))
                } else if name.eq_ignore_ascii_case("max_allowed_packet") {
                    Ok(Value::Int(67108864))
                } else if name.eq_ignore_ascii_case("lower_case_table_names") {
                    Ok(Value::Int(0))
                } else if name.eq_ignore_ascii_case("wait_timeout")
                    || name.eq_ignore_ascii_case("interactive_timeout")
                {
                    // Real MySQL default (seconds); client pools (e.g.
                    // mysql_async's connection setup probe) parse this as
                    // a number, so it can't be the empty-text fallback.
                    Ok(Value::Int(28800))
                } else if name.eq_ignore_ascii_case("socket") {
                    Ok(Value::Text("/tmp/mysql.sock".to_string()))
                } else {
                    Ok(Value::Text("".to_string()))
                }
            }
            _ => Err(MySqlError::unsupported("expr in execution")),
        }
    }
}

/// Basic integer/float arithmetic. Only defined for the two numeric
/// `Value` variants this engine's literal parser actually produces
/// (`Int`/`Float`) — mixing in a float promotes the result to float,
/// matching MySQL's own numeric-promotion rule for `+ - * /`.
fn eval_arith(op: ArithOp, l: Value, r: Value) -> Result<Value, MySqlError> {
    let as_f64 = |v: &Value| match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    };
    match (&l, &r) {
        (Value::Int(a), Value::Int(b)) => match op {
            ArithOp::Add => Ok(Value::Int(a + b)),
            ArithOp::Sub => Ok(Value::Int(a - b)),
            ArithOp::Mul => Ok(Value::Int(a * b)),
            // MySQL's `/` on two integers returns a DECIMAL, scaled by
            // `div_precision_increment` (default 4) beyond the operands'
            // scale (0 for an integer) — i.e. always exactly 4 decimal
            // places, not a bare float's shortest representation
            // (`10/4` is "2.5000", never "2.5").
            ArithOp::Div => {
                if *b == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(format!("{:.4}", *a as f64 / *b as f64)))
                }
            }
        },
        _ => {
            let (Some(a), Some(b)) = (as_f64(&l), as_f64(&r)) else {
                return Err(MySqlError::new(
                    1292,
                    "22007",
                    format!("Truncated incorrect DOUBLE value: {:?}", l),
                ));
            };
            match op {
                ArithOp::Add => Ok(Value::Float(a + b)),
                ArithOp::Sub => Ok(Value::Float(a - b)),
                ArithOp::Mul => Ok(Value::Float(a * b)),
                ArithOp::Div => {
                    if b == 0.0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Float(a / b))
                    }
                }
            }
        }
    }
}

/// MySQL's three-valued comparison logic: NULL on either side always
/// yields NULL (never true or false), never an error. Result is 0/1,
/// matching how MySQL represents boolean results.
fn eval_compare(op: CmpOp, l: Value, r: Value) -> Result<Value, MySqlError> {
    if l.is_null() || r.is_null() {
        return Ok(Value::Null);
    }
    let ordering = match (&l, &r) {
        (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
        _ => {
            let as_f64 = |v: &Value| match v {
                Value::Int(i) => Some(*i as f64),
                Value::Float(f) => Some(*f),
                _ => None,
            };
            match (as_f64(&l), as_f64(&r)) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => {
                    return Err(MySqlError::new(
                        1292,
                        "22007",
                        format!("Truncated incorrect DOUBLE value: {:?}", l),
                    ));
                }
            }
        }
    };
    let Some(ordering) = ordering else {
        return Ok(Value::Null);
    };
    let result = match op {
        CmpOp::Eq => ordering == Ordering::Equal,
        CmpOp::Ne => ordering != Ordering::Equal,
        CmpOp::Lt => ordering == Ordering::Less,
        CmpOp::Le => ordering != Ordering::Greater,
        CmpOp::Gt => ordering == Ordering::Greater,
        CmpOp::Ge => ordering != Ordering::Less,
    };
    Ok(Value::Int(result as i64))
}
