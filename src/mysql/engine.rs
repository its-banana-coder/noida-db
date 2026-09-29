use crate::mysql::binder::Binder;
use crate::mysql::exec::Executor;
use crate::mysql::types::Value;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

#[derive(Default)]
pub struct Engine {
    binder: Binder,
    executor: Executor,
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execute(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let dialect = MySqlDialect {};
        let mut asts = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

        if asts.is_empty() {
            return Ok(vec![]);
        }

        let stmt = asts.remove(0);
        let plan = self.binder.bind_statement(stmt)?;
        self.executor.execute_plan(plan)
    }

    pub fn use_db(&mut self, db: &str) {
        self.binder.current_db = Some(db.to_string());
        self.executor.current_db = Some(db.to_string());
    }
}
