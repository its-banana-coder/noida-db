use crate::mysql::plan::{ArithOp, CmpOp, Expr, Plan};
use crate::mysql::types::Value;
use std::cmp::Ordering;

#[derive(Default)]
pub struct Executor {
    pub current_db: Option<String>,
}

impl Executor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execute_plan(&mut self, plan: Plan) -> Result<Vec<Vec<Value>>, String> {
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
                self.current_db = Some(db);
                Ok(vec![])
            }
            Plan::ShowTables(_db) => Ok(vec![]),
            Plan::Project { source, exprs, .. } => {
                let rows = self.execute_plan(*source)?;
                let mut out_rows = Vec::new();
                for _row in rows {
                    let mut out_row = Vec::new();
                    for expr in &exprs {
                        out_row.push(self.eval_expr(expr)?);
                    }
                    out_rows.push(out_row);
                }
                Ok(out_rows)
            }
            _ => Err("unsupported plan in execution".to_string()),
        }
    }

    fn eval_expr(&self, expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::Const(v) => Ok(v.clone()),
            Expr::Arith { op, left, right } => {
                let l = self.eval_expr(left)?;
                let r = self.eval_expr(right)?;
                eval_arith(*op, l, r)
            }
            Expr::Compare { op, left, right } => {
                let l = self.eval_expr(left)?;
                let r = self.eval_expr(right)?;
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
            _ => Err("unsupported expr in execution".to_string()),
        }
    }
}

/// Basic integer/float arithmetic. Only defined for the two numeric
/// `Value` variants this engine's literal parser actually produces
/// (`Int`/`Float`) — mixing in a float promotes the result to float,
/// matching MySQL's own numeric-promotion rule for `+ - * /`.
fn eval_arith(op: ArithOp, l: Value, r: Value) -> Result<Value, String> {
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
                return Err(format!("non-numeric operand in arithmetic: {l:?}, {r:?}"));
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
fn eval_compare(op: CmpOp, l: Value, r: Value) -> Result<Value, String> {
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
                _ => return Err(format!("cannot compare {l:?} and {r:?}")),
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
