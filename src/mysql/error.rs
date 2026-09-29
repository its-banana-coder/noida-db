use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct MySqlError {
    pub code: u16,
    pub sql_state: &'static str,
    pub message: String,
}

impl MySqlError {
    pub fn new(code: u16, sql_state: &'static str, message: impl Into<String>) -> Self {
        Self { code, sql_state, message: message.into() }
    }

    pub fn unknown_table(table: &str) -> Self {
        Self::new(1146, "42S02", format!("Table '{}' doesn't exist", table))
    }

    pub fn unknown_column(col: &str) -> Self {
        Self::new(1054, "42S22", format!("Unknown column '{}' in 'field list'", col))
    }

    pub fn duplicate_key(key: &str) -> Self {
        Self::new(1062, "23000", format!("Duplicate entry for key '{}'", key))
    }

    pub fn syntax_error(msg: &str) -> Self {
        Self::new(1064, "42000", format!("You have an error in your SQL syntax; {}", msg))
    }

    pub fn unsupported(msg: &str) -> Self {
        Self::new(1235, "42000", format!("This version of MySQL doesn't yet support '{}'", msg))
    }
}

impl fmt::Display for MySqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ERROR {} ({}): {}", self.code, self.sql_state, self.message)
    }
}
