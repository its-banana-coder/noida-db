use crate::mysql::binder::Binder;
use crate::mysql::catalog::DbState;
use crate::mysql::error::MySqlError;
use crate::mysql::exec::Executor;
use crate::mysql::types::Value;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Engine {
    pub db: Arc<Mutex<DbState>>,
    pub current_db: Option<String>,
}

impl Default for Engine {
    fn default() -> Self {
        Self { db: Arc::new(Mutex::new(DbState::default())), current_db: None }
    }
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execute(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, MySqlError> {
        let dialect = MySqlDialect {};
        let mut asts = Parser::parse_sql(&dialect, sql)
            .map_err(|e| MySqlError::syntax_error(&e.to_string()))?;

        if asts.is_empty() {
            return Ok(vec![]);
        }

        let stmt = asts.remove(0);

        let mut binder = Binder::new(self.current_db.clone());
        let plan = binder.bind_statement(stmt)?;

        let mut executor = Executor::new(self.db.clone(), self.current_db.clone());
        let res = executor.execute_plan(plan)?;

        // Update current DB if USE was called
        if let Some(db) = executor.current_db {
            self.current_db = Some(db);
        }

        Ok(res)
    }

    pub fn use_db(&mut self, db: &str) {
        self.current_db = Some(db.to_string());
    }
}
