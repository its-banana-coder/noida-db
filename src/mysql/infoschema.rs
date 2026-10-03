//! `information_schema` as read-only virtual tables, built from the catalog
//! on every read. ORMs and migration tools (Laravel's `hasTable`/
//! `getColumnListing`, Prisma/Django introspection, Flyway/Liquibase) ask
//! these instead of `SHOW ...`.

use std::sync::Arc;

use crate::mysql::catalog::{Column, ColumnType, DbState, Table};
use crate::mysql::exec::{mysql_type_name, render_text};
use crate::mysql::types::Value;

/// A real table, or one of the virtual `information_schema` tables
/// (matched case-insensitively, as MySQL does for that schema).
pub fn lookup_table(state: &DbState, db: &str, table: &str) -> Option<Arc<Table>> {
    if db.eq_ignore_ascii_case("information_schema") {
        return virtual_table(state, &table.to_ascii_uppercase()).map(Arc::new);
    }
    state.schemas.get(db)?.tables.get(table).cloned()
}

fn text(s: &str) -> Value {
    Value::Text(s.to_string())
}

fn table_of(name: &str, cols: &[&str], rows: Vec<Vec<Value>>) -> Table {
    let columns = cols
        .iter()
        .map(|c| Column {
            name: c.to_string(),
            ty: ColumnType::Text,
            not_null: false,
            default: None,
            auto_increment: false,
            primary_key: false,
            default_now: false,
            on_update_now: false,
        })
        .collect();
    let mut t = Table::new(name.to_string(), columns);
    t.rows = rows;
    t
}

/// Every user table, as (schema, table).
fn user_tables(state: &DbState) -> impl Iterator<Item = (&String, &Arc<Table>)> {
    state.schemas.iter().flat_map(|(db, s)| s.tables.values().map(move |t| (db, t)))
}

fn virtual_table(state: &DbState, name: &str) -> Option<Table> {
    Some(match name {
        "SCHEMATA" => table_of(
            name,
            &[
                "CATALOG_NAME",
                "SCHEMA_NAME",
                "DEFAULT_CHARACTER_SET_NAME",
                "DEFAULT_COLLATION_NAME",
            ],
            state
                .schemas
                .keys()
                .map(|s| vec![text("def"), text(s), text("utf8mb4"), text("utf8mb4_0900_ai_ci")])
                .collect(),
        ),
        "TABLES" => table_of(
            name,
            &[
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "TABLE_TYPE",
                "ENGINE",
                "VERSION",
                "ROW_FORMAT",
                "TABLE_ROWS",
                "AVG_ROW_LENGTH",
                "DATA_LENGTH",
                "INDEX_LENGTH",
                "AUTO_INCREMENT",
                "CREATE_TIME",
                "UPDATE_TIME",
                "TABLE_COLLATION",
                "TABLE_COMMENT",
            ],
            user_tables(state)
                .map(|(db, t)| {
                    let has_ai = t.columns.iter().any(|c| c.auto_increment);
                    vec![
                        text("def"),
                        text(db),
                        text(&t.name),
                        text("BASE TABLE"),
                        text("InnoDB"),
                        Value::Int(10),
                        text("Dynamic"),
                        Value::Int(t.rows.len() as i64),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        if has_ai { Value::Int(t.next_auto_increment) } else { Value::Null },
                        Value::Null,
                        Value::Null,
                        text("utf8mb4_0900_ai_ci"),
                        text(""),
                    ]
                })
                .collect(),
        ),
        "COLUMNS" => {
            let mut rows = Vec::new();
            for (db, t) in user_tables(state) {
                let keys = t.keys();
                for (i, c) in t.columns.iter().enumerate() {
                    let full = mysql_type_name(&c.ty);
                    let data_type = full.split('(').next().unwrap_or(&full).to_string();
                    let (char_len, num_prec, num_scale) = match &c.ty {
                        ColumnType::Varchar(n) => (Value::Int(*n as i64), Value::Null, Value::Null),
                        ColumnType::Text => (Value::Int(65535), Value::Null, Value::Null),
                        ColumnType::Int => (Value::Null, Value::Int(10), Value::Int(0)),
                        ColumnType::BigInt => (Value::Null, Value::Int(19), Value::Int(0)),
                        ColumnType::Boolean => (Value::Null, Value::Int(3), Value::Int(0)),
                        ColumnType::Decimal(p, s) => {
                            (Value::Null, Value::Int(*p as i64), Value::Int(*s as i64))
                        }
                        ColumnType::Double => (Value::Null, Value::Int(22), Value::Null),
                        ColumnType::Float => (Value::Null, Value::Int(12), Value::Null),
                        _ => (Value::Null, Value::Null, Value::Null),
                    };
                    let is_text = matches!(
                        c.ty,
                        ColumnType::Varchar(_) | ColumnType::Text | ColumnType::Enum(_)
                    );
                    let key = if c.primary_key {
                        "PRI"
                    } else if keys.iter().any(|(_, k)| k.len() == 1 && k[0] == i) {
                        "UNI"
                    } else {
                        ""
                    };
                    let default = if c.default_now {
                        text("CURRENT_TIMESTAMP")
                    } else {
                        match &c.default {
                            None | Some(Value::Null) => Value::Null,
                            Some(v) => Value::Text(render_text(v)),
                        }
                    };
                    let extra = if c.auto_increment {
                        "auto_increment"
                    } else if c.on_update_now {
                        "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
                    } else if c.default_now {
                        "DEFAULT_GENERATED"
                    } else {
                        ""
                    };
                    rows.push(vec![
                        text("def"),
                        text(db),
                        text(&t.name),
                        text(&c.name),
                        Value::Int(i as i64 + 1),
                        default,
                        text(if c.not_null { "NO" } else { "YES" }),
                        text(&data_type),
                        char_len,
                        num_prec,
                        num_scale,
                        if matches!(c.ty, ColumnType::Datetime) {
                            Value::Int(0)
                        } else {
                            Value::Null
                        },
                        if is_text { text("utf8mb4") } else { Value::Null },
                        if is_text { text("utf8mb4_0900_ai_ci") } else { Value::Null },
                        text(&full),
                        text(key),
                        text(extra),
                        text(""),
                        text(""),
                    ]);
                }
            }
            table_of(
                name,
                &[
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE",
                    "CHARACTER_MAXIMUM_LENGTH",
                    "NUMERIC_PRECISION",
                    "NUMERIC_SCALE",
                    "DATETIME_PRECISION",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    "COLUMN_TYPE",
                    "COLUMN_KEY",
                    "EXTRA",
                    "COLUMN_COMMENT",
                    "GENERATION_EXPRESSION",
                ],
                rows,
            )
        }
        // One row per (key, column), PRIMARY and UNIQUE keys only -- plain
        // `KEY`/`INDEX` declarations aren't tracked.
        "STATISTICS" | "KEY_COLUMN_USAGE" => {
            let mut rows = Vec::new();
            for (db, t) in user_tables(state) {
                for (key, cols) in t.keys() {
                    for (seq, &c) in cols.iter().enumerate() {
                        let col = &t.columns[c].name;
                        rows.push(if name == "STATISTICS" {
                            vec![
                                text("def"),
                                text(db),
                                text(&t.name),
                                Value::Int(0),
                                text(&key),
                                Value::Int(seq as i64 + 1),
                                text(col),
                                text("BTREE"),
                            ]
                        } else {
                            vec![
                                text("def"),
                                text(db),
                                text(&key),
                                text("def"),
                                text(db),
                                text(&t.name),
                                text(col),
                                Value::Int(seq as i64 + 1),
                                Value::Null,
                                Value::Null,
                                Value::Null,
                            ]
                        });
                    }
                }
            }
            if name == "STATISTICS" {
                table_of(
                    name,
                    &[
                        "TABLE_CATALOG",
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "NON_UNIQUE",
                        "INDEX_NAME",
                        "SEQ_IN_INDEX",
                        "COLUMN_NAME",
                        "INDEX_TYPE",
                    ],
                    rows,
                )
            } else {
                table_of(
                    name,
                    &[
                        "CONSTRAINT_CATALOG",
                        "CONSTRAINT_SCHEMA",
                        "CONSTRAINT_NAME",
                        "TABLE_CATALOG",
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "COLUMN_NAME",
                        "ORDINAL_POSITION",
                        "REFERENCED_TABLE_SCHEMA",
                        "REFERENCED_TABLE_NAME",
                        "REFERENCED_COLUMN_NAME",
                    ],
                    rows,
                )
            }
        }
        _ => return None,
    })
}

/// `SHOW INDEX FROM t`'s rows, in MySQL's 15-column shape.
pub fn show_index(t: &Table) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (key, cols) in t.keys() {
        for (seq, &c) in cols.iter().enumerate() {
            let col = &t.columns[c];
            rows.push(vec![
                text(&t.name),
                Value::Int(0),
                text(&key),
                Value::Int(seq as i64 + 1),
                text(&col.name),
                text("A"),
                Value::Int(t.rows.len() as i64),
                Value::Null,
                Value::Null,
                text(if col.not_null { "" } else { "YES" }),
                text("BTREE"),
                text(""),
                text(""),
                text("YES"),
                Value::Null,
            ]);
        }
    }
    rows
}

pub const SHOW_INDEX_COLUMNS: [&str; 15] = [
    "Table",
    "Non_unique",
    "Key_name",
    "Seq_in_index",
    "Column_name",
    "Collation",
    "Cardinality",
    "Sub_part",
    "Packed",
    "Null",
    "Index_type",
    "Comment",
    "Index_comment",
    "Visible",
    "Expression",
];
