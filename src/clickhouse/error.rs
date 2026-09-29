//! ClickHouse's exception codes and the HTTP error body format.
//!
//! Real error bodies look like:
//! `Code: 60. DB::Exception: Unknown table expression identifier 'x' in
//! scope SELECT * FROM x. (UNKNOWN_TABLE) (version 24.8.4.13 (official
//! build))`

use super::engine::SERVER_VERSION;
use super::sql::SqlError;

#[derive(Debug, Clone, PartialEq)]
pub struct ChError {
    pub code: u32,
    pub name: &'static str,
    pub message: String,
}

impl ChError {
    pub fn new(code: u32, name: &'static str, message: impl Into<String>) -> ChError {
        ChError { code, name, message: message.into() }
    }

    pub fn syntax(message: impl Into<String>) -> ChError {
        ChError::new(62, "SYNTAX_ERROR", message)
    }

    pub fn unknown_function(name: &str) -> ChError {
        ChError::new(46, "UNKNOWN_FUNCTION", format!("Unknown function {name}"))
    }

    pub fn unknown_identifier(name: &str) -> ChError {
        ChError::new(47, "UNKNOWN_IDENTIFIER", format!("Unknown identifier '{name}'"))
    }

    pub fn unknown_database(database: &str) -> ChError {
        ChError::new(81, "UNKNOWN_DATABASE", format!("Database {database} doesn't exist"))
    }

    pub fn unknown_table(database: &str, table: &str) -> ChError {
        if database != "system" && database != "default" {
            return ChError::unknown_database(database);
        }
        ChError::new(
            60,
            "UNKNOWN_TABLE",
            format!(
                "Unknown table expression identifier '{table}' in scope \
                 SELECT * FROM {database}.{table}."
            ),
        )
    }

    pub fn not_implemented(what: &str) -> ChError {
        ChError::new(48, "NOT_IMPLEMENTED", format!("{what} is not supported"))
    }

    pub fn table_already_exists(database: &str, table: &str) -> ChError {
        ChError::new(57, "TABLE_ALREADY_EXISTS", format!("Table {database}.{table} already exists"))
    }

    pub fn type_mismatch(type_name: &str) -> ChError {
        ChError::new(53, "TYPE_MISMATCH", format!("Cannot convert value to type {type_name}"))
    }

    pub fn out_of_range(type_name: &str) -> ChError {
        ChError::new(
            69,
            "ARGUMENT_OUT_OF_BOUND",
            format!("Value is out of range of type {type_name}"),
        )
    }

    pub fn no_common_type(a: &str, b: &str) -> ChError {
        ChError::new(386, "NO_COMMON_TYPE", format!("Cannot compare {a} and {b}: no common type"))
    }

    pub fn division_by_zero() -> ChError {
        ChError::new(153, "ILLEGAL_DIVISION", "Division by zero")
    }

    /// The HTTP status ClickHouse's HTTP interface uses for this exception.
    pub fn http_status(&self) -> u16 {
        match self.code {
            60 | 81 => 404,
            _ => 500,
        }
    }

    /// The body ClickHouse's HTTP interface sends for this exception.
    pub fn body(&self) -> String {
        format!(
            "Code: {}. DB::Exception: {} ({}) (version {} (official build))\n",
            self.code, self.message, self.name, SERVER_VERSION
        )
    }
}

impl From<SqlError> for ChError {
    fn from(e: SqlError) -> ChError {
        ChError::syntax(e.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_table_body_matches_clickhouse_format() {
        let e = ChError::unknown_table("system", "nope");
        assert_eq!(
            e.body(),
            "Code: 60. DB::Exception: Unknown table expression identifier 'nope' in scope \
             SELECT * FROM system.nope. (UNKNOWN_TABLE) (version 24.8.4.13 (official build))\n"
        );
        assert_eq!(e.http_status(), 404);
    }

    #[test]
    fn unknown_database_routes_through_unknown_table() {
        let e = ChError::unknown_table("nosuchdb", "t");
        assert_eq!(e.code, 81);
        assert_eq!(e.name, "UNKNOWN_DATABASE");
    }

    #[test]
    fn other_errors_are_500() {
        assert_eq!(ChError::syntax("bad").http_status(), 500);
        assert_eq!(ChError::unknown_function("nope").http_status(), 500);
    }
}
