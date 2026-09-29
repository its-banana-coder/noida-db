use crate::mysql::plan::Plan;
use crate::mysql::types::Value;

pub struct Executor {
    pub current_db: Option<String>,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            current_db: None,
        }
    }

    pub fn execute_plan(&mut self, plan: Plan) -> Result<Vec<Vec<Value>>, String> {
        match plan {
            Plan::Dummy => {
                Ok(vec![vec![]])
            }
            Plan::ShowDatabases => {
                Ok(vec![
                    vec![Value::Text("information_schema".to_string())],
                    vec![Value::Text("mysql".to_string())],
                    vec![Value::Text("performance_schema".to_string())],
                    vec![Value::Text("sys".to_string())],
                    vec![Value::Text("test".to_string())],
                ])
            }
            Plan::Use(db) => {
                self.current_db = Some(db);
                Ok(vec![])
            }
            Plan::ShowTables(_db) => {
                Ok(vec![])
            }
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

    fn eval_expr(&self, expr: &crate::mysql::plan::Expr) -> Result<Value, String> {
        match expr {
            crate::mysql::plan::Expr::Const(v) => Ok(v.clone()),
            crate::mysql::plan::Expr::SysVar(name) => {
                if name.eq_ignore_ascii_case("version") {
                    Ok(Value::Text("8.0.33".to_string()))
                } else if name.eq_ignore_ascii_case("version_comment") {
                    Ok(Value::Text("noida-db MySQL 8.0".to_string()))
                } else if name.eq_ignore_ascii_case("max_allowed_packet") {
                    Ok(Value::Int(67108864))
                } else if name.eq_ignore_ascii_case("lower_case_table_names") {
                    Ok(Value::Int(0))
                } else {
                    Ok(Value::Text("".to_string()))
                }
            }
            _ => Err("unsupported expr in execution".to_string()),
        }
    }
}
