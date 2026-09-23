//! Errors carrying a SQLSTATE, sent to clients as ErrorResponse.

use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct PgError {
    pub severity: &'static str,
    pub code: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub constraint: Option<String>,
    pub datatype: Option<String>,
}

pub type PgResult<T> = Result<T, PgError>;

impl PgError {
    pub fn new(code: &'static str, message: impl Into<String>) -> PgError {
        PgError {
            severity: "ERROR",
            code,
            message: message.into(),
            detail: None,
            hint: None,
            position: None,
            schema: None,
            table: None,
            column: None,
            constraint: None,
            datatype: None,
        }
    }

    pub fn fatal(code: &'static str, message: impl Into<String>) -> PgError {
        PgError { severity: "FATAL", ..PgError::new(code, message) }
    }

    pub fn detail(mut self, d: impl Into<String>) -> PgError {
        self.detail = Some(d.into());
        self
    }

    pub fn hint(mut self, h: impl Into<String>) -> PgError {
        self.hint = Some(h.into());
        self
    }

    pub fn table(mut self, schema: &str, table: &str) -> PgError {
        self.schema = Some(schema.to_string());
        self.table = Some(table.to_string());
        self
    }

    pub fn column(mut self, c: &str) -> PgError {
        self.column = Some(c.to_string());
        self
    }

    pub fn constraint(mut self, c: &str) -> PgError {
        self.constraint = Some(c.to_string());
        self
    }

    /// ErrorResponse / NoticeResponse fields.
    pub fn fields(&self) -> Vec<(u8, String)> {
        let mut f = vec![
            (b'S', self.severity.to_string()),
            (b'V', self.severity.to_string()),
            (b'C', self.code.to_string()),
            (b'M', self.message.clone()),
        ];
        let opt = [
            (b'D', &self.detail),
            (b'H', &self.hint),
            (b's', &self.schema),
            (b't', &self.table),
            (b'c', &self.column),
            (b'd', &self.datatype),
            (b'n', &self.constraint),
        ];
        for (k, v) in opt {
            if let Some(v) = v {
                f.push((k, v.clone()));
            }
        }
        if let Some(p) = self.position {
            f.push((b'P', p.to_string()));
        }
        f
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} ({})", self.severity, self.message, self.code)
    }
}

/// SQLSTATE codes used across the engine.
pub mod code {
    pub const SUCCESSFUL: &str = "00000";
    pub const WARNING: &str = "01000";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
    pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
    pub const NULL_VALUE_NOT_ALLOWED: &str = "22004";
    pub const INVALID_DATETIME_FORMAT: &str = "22007";
    pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
    pub const INVALID_TIME_ZONE_DISPLACEMENT: &str = "22009";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const INVALID_ESCAPE_SEQUENCE: &str = "22025";
    pub const ARRAY_SUBSCRIPT_ERROR: &str = "2202E";
    pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
    pub const INVALID_BINARY_REPRESENTATION: &str = "22P03";
    pub const INVALID_REGULAR_EXPRESSION: &str = "2201B";
    pub const INVALID_ROW_COUNT_IN_LIMIT: &str = "2201W";
    pub const INVALID_ROW_COUNT_IN_OFFSET: &str = "2201X";
    pub const INVALID_ARGUMENT_FOR_LOG: &str = "2201E";
    pub const INVALID_ARGUMENT_FOR_POWER: &str = "2201F";
    pub const INVALID_ARGUMENT_FOR_WIDTH_BUCKET: &str = "2201G";
    pub const SUBSTRING_ERROR: &str = "22011";
    pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021";
    pub const UNTRANSLATABLE_CHARACTER: &str = "22P05";
    pub const INVALID_JSON_TEXT: &str = "22P02";
    pub const CARDINALITY_VIOLATION: &str = "21000";
    pub const NOT_NULL_VIOLATION: &str = "23502";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const CHECK_VIOLATION: &str = "23514";
    pub const INVALID_CURSOR_STATE: &str = "24000";
    pub const INVALID_TRANSACTION_STATE: &str = "25000";
    pub const ACTIVE_SQL_TRANSACTION: &str = "25001";
    pub const NO_ACTIVE_SQL_TRANSACTION: &str = "25P01";
    pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02";
    pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const INVALID_AUTHORIZATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const INVALID_CURSOR_NAME: &str = "34000";
    pub const INVALID_CATALOG_NAME: &str = "3D000";
    pub const INVALID_SCHEMA_NAME: &str = "3F000";
    pub const S_E_INVALID_SPECIFICATION: &str = "3B001";
    pub const SERIALIZATION_FAILURE: &str = "40001";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
    pub const GROUPING_ERROR: &str = "42803";
    pub const INVALID_FOREIGN_KEY: &str = "42830";
    pub const WRONG_OBJECT_TYPE: &str = "42809";
    pub const INVALID_COLUMN_REFERENCE: &str = "42P10";
    pub const UNDEFINED_COLUMN: &str = "42703";
    pub const UNDEFINED_FUNCTION: &str = "42883";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_PARAMETER: &str = "42P02";
    pub const UNDEFINED_OBJECT: &str = "42704";
    pub const DUPLICATE_COLUMN: &str = "42701";
    pub const DUPLICATE_CURSOR: &str = "42P03";
    pub const DUPLICATE_DATABASE: &str = "42P04";
    pub const DUPLICATE_PSTATEMENT: &str = "42P05";
    pub const DUPLICATE_SCHEMA: &str = "42P06";
    pub const DUPLICATE_TABLE: &str = "42P07";
    pub const DUPLICATE_ALIAS: &str = "42712";
    pub const DUPLICATE_OBJECT: &str = "42710";
    pub const AMBIGUOUS_COLUMN: &str = "42702";
    pub const AMBIGUOUS_FUNCTION: &str = "42725";
    pub const AMBIGUOUS_PARAMETER: &str = "42P08";
    pub const INDETERMINATE_DATATYPE: &str = "42P18";
    pub const DATATYPE_MISMATCH: &str = "42804";
    pub const CANNOT_COERCE: &str = "42846";
    pub const INVALID_TABLE_DEFINITION: &str = "42P16";
    pub const INVALID_OBJECT_DEFINITION: &str = "42P17";
    pub const INVALID_NAME: &str = "42602";
    pub const NAME_TOO_LONG: &str = "42622";
    pub const RESERVED_NAME: &str = "42939";
    pub const WINDOWING_ERROR: &str = "42P20";
    pub const INVALID_RECURSION: &str = "42P19";
    pub const DEPENDENT_OBJECTS_STILL_EXIST: &str = "2BP01";
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";
    pub const OBJECT_IN_USE: &str = "55006";
    pub const QUERY_CANCELED: &str = "57014";
    pub const ADMIN_SHUTDOWN: &str = "57P01";
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const CANT_CHANGE_RUNTIME_PARAM: &str = "55P02";
    pub const RAISE_EXCEPTION: &str = "P0001";
    pub const INTERNAL_ERROR: &str = "XX000";
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const TOO_MANY_COLUMNS: &str = "54011";
    pub const STATEMENT_TOO_COMPLEX: &str = "54001";
    pub const CONFIGURATION_LIMIT_EXCEEDED: &str = "53400";
    pub const UNDEFINED_PSTATEMENT: &str = "26000";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
}

/// `feature_not_supported` in Postgres's usual wording.
pub fn unsupported(what: impl fmt::Display) -> PgError {
    PgError::new(code::FEATURE_NOT_SUPPORTED, format!("{what} is not supported"))
}
