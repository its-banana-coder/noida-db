//! Schema objects and table data. A [`DbState`] is cheap to clone (tables
//! sit behind `Arc`s), which is how transactions snapshot and roll back.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::error::{PgError, PgResult, code};
use super::types::{Type, Value};

pub type Row = Vec<Value>;

/// Well-known OIDs, matching Postgres so catalog queries line up.
pub const PG_CATALOG_NS: u32 = 11;
pub const PUBLIC_NS: u32 = 2200;
pub const INFORMATION_SCHEMA_NS: u32 = 13000;
pub const PG_TOAST_NS: u32 = 99;
pub const BOOTSTRAP_SUPERUSER: u32 = 10;
pub const DATABASE_OID: u32 = 16384;
/// First OID handed to user objects.
pub const FIRST_USER_OID: u32 = 16385;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub typmod: i32,
    pub not_null: bool,
    /// Default expression as SQL text (what pg_get_expr prints).
    pub default: Option<String>,
    /// `GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY`: (always, sequence oid).
    pub identity: Option<(bool, u32)>,
    /// `GENERATED ALWAYS AS (expr) STORED`.
    pub generated: Option<String>,
    pub dropped: bool,
    pub comment: Option<String>,
}

impl Column {
    pub fn new(name: &str, ty: Type) -> Column {
        Column {
            name: name.to_string(),
            ty,
            typmod: -1,
            not_null: false,
            default: None,
            identity: None,
            generated: None,
            dropped: false,
            comment: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FkAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl FkAction {
    pub fn code(&self) -> char {
        match self {
            FkAction::NoAction => 'a',
            FkAction::Restrict => 'r',
            FkAction::Cascade => 'c',
            FkAction::SetNull => 'n',
            FkAction::SetDefault => 'd',
        }
    }
    pub fn sql(&self) -> &'static str {
        match self {
            FkAction::NoAction => "NO ACTION",
            FkAction::Restrict => "RESTRICT",
            FkAction::Cascade => "CASCADE",
            FkAction::SetNull => "SET NULL",
            FkAction::SetDefault => "SET DEFAULT",
        }
    }
}

#[derive(Clone, Debug)]
pub enum ConstraintKind {
    PrimaryKey,
    Unique,
    /// SQL text of the check expression.
    Check(String),
    ForeignKey {
        ref_table: u32,
        ref_cols: Vec<usize>,
        on_delete: FkAction,
        on_update: FkAction,
    },
}

#[derive(Clone, Debug)]
pub struct Constraint {
    pub oid: u32,
    pub name: String,
    pub kind: ConstraintKind,
    /// Column positions (0-based) in the owning table.
    pub cols: Vec<usize>,
    /// Backing index for PK/UNIQUE.
    pub index_oid: Option<u32>,
    pub deferrable: bool,
    pub comment: Option<String>,
}

impl Constraint {
    pub fn contype(&self) -> char {
        match self.kind {
            ConstraintKind::PrimaryKey => 'p',
            ConstraintKind::Unique => 'u',
            ConstraintKind::Check(_) => 'c',
            ConstraintKind::ForeignKey { .. } => 'f',
        }
    }
}

#[derive(Clone, Debug)]
pub struct Index {
    pub oid: u32,
    pub name: String,
    /// Key columns; `None` for an expression key (text in `exprs`).
    pub cols: Vec<Option<usize>>,
    pub exprs: Vec<String>,
    pub unique: bool,
    pub primary: bool,
    /// Descending flags per key.
    pub desc: Vec<bool>,
    /// Partial index predicate SQL.
    pub predicate: Option<String>,
    pub method: String,
    pub nulls_not_distinct: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RelKind {
    Table,
    View,
    MaterializedView,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub oid: u32,
    pub name: String,
    pub schema: u32,
    pub kind: RelKind,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    /// View definition (SELECT text) for views.
    pub view_sql: Option<String>,
    pub comment: Option<String>,
    /// Composite row type OID (pg_type entry for the table).
    pub type_oid: u32,
    pub temp: bool,
    pub owner_session: Option<u32>,
}

impl Table {
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| !c.dropped && c.name == name)
    }

    pub fn live_columns(&self) -> impl Iterator<Item = (usize, &Column)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.dropped)
    }

    pub fn primary_key(&self) -> Option<&Constraint> {
        self.constraints.iter().find(|c| matches!(c.kind, ConstraintKind::PrimaryKey))
    }

    pub fn relkind(&self) -> char {
        match self.kind {
            RelKind::Table => 'r',
            RelKind::View => 'v',
            RelKind::MaterializedView => 'm',
        }
    }
}

#[derive(Clone, Debug)]
pub struct Sequence {
    pub oid: u32,
    pub name: String,
    pub schema: u32,
    pub ty: Type,
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cache: i64,
    pub cycle: bool,
    /// (table oid, column index) for serial/identity/OWNED BY.
    pub owned_by: Option<(u32, usize)>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub oid: u32,
    pub name: String,
    pub owner: u32,
    pub comment: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EnumType {
    pub oid: u32,
    pub name: String,
    pub schema: u32,
    /// (sort order, label, pg_enum oid)
    pub labels: Vec<(f32, String, u32)>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Domain {
    pub oid: u32,
    pub name: String,
    pub schema: u32,
    pub base: Type,
    pub typmod: i32,
    pub not_null: bool,
    pub default: Option<String>,
    pub checks: Vec<(String, String)>,
}

/// Sequence counters live outside transactional state: nextval is never
/// rolled back.
#[derive(Clone, Debug, Default)]
pub struct SeqValue {
    pub last: i64,
    pub is_called: bool,
}

/// All transactional schema and data.
#[derive(Clone, Debug)]
pub struct DbState {
    pub schemas: BTreeMap<u32, Schema>,
    pub tables: BTreeMap<u32, Arc<Table>>,
    pub sequences: BTreeMap<u32, Sequence>,
    pub enums: BTreeMap<u32, EnumType>,
    pub domains: BTreeMap<u32, Domain>,
    pub next_oid: u32,
    pub db_comment: Option<String>,
}

impl Default for DbState {
    fn default() -> Self {
        let mut schemas = BTreeMap::new();
        for (oid, name) in [
            (PG_CATALOG_NS, "pg_catalog"),
            (PUBLIC_NS, "public"),
            (INFORMATION_SCHEMA_NS, "information_schema"),
            (PG_TOAST_NS, "pg_toast"),
        ] {
            let comment = match name {
                "public" => Some("standard public schema".to_string()),
                "pg_catalog" => Some("system catalog schema".to_string()),
                _ => None,
            };
            schemas.insert(
                oid,
                Schema { oid, name: name.to_string(), owner: BOOTSTRAP_SUPERUSER, comment },
            );
        }
        DbState {
            schemas,
            tables: BTreeMap::new(),
            sequences: BTreeMap::new(),
            enums: BTreeMap::new(),
            domains: BTreeMap::new(),
            next_oid: FIRST_USER_OID,
            db_comment: None,
        }
    }
}

impl DbState {
    pub fn alloc_oid(&mut self) -> u32 {
        let o = self.next_oid;
        self.next_oid += 1;
        o
    }

    pub fn schema_by_name(&self, name: &str) -> Option<u32> {
        self.schemas.values().find(|s| s.name == name).map(|s| s.oid)
    }

    pub fn schema_name(&self, oid: u32) -> &str {
        self.schemas.get(&oid).map_or("?", |s| s.name.as_str())
    }

    pub fn table(&self, oid: u32) -> Option<&Table> {
        self.tables.get(&oid).map(|t| t.as_ref())
    }

    pub fn table_mut(&mut self, oid: u32) -> Option<&mut Table> {
        self.tables.get_mut(&oid).map(Arc::make_mut)
    }

    pub fn find_table(&self, schema: u32, name: &str) -> Option<&Table> {
        self.tables.values().find(|t| t.schema == schema && t.name == name).map(|t| t.as_ref())
    }

    pub fn find_sequence(&self, schema: u32, name: &str) -> Option<&Sequence> {
        self.sequences.values().find(|s| s.schema == schema && s.name == name)
    }

    pub fn find_enum(&self, schema: u32, name: &str) -> Option<&EnumType> {
        self.enums.values().find(|e| e.schema == schema && e.name == name)
    }

    pub fn find_domain(&self, schema: u32, name: &str) -> Option<&Domain> {
        self.domains.values().find(|e| e.schema == schema && e.name == name)
    }

    /// Any relation-like name (table, view, sequence, index) in a schema.
    pub fn relation_exists(&self, schema: u32, name: &str) -> bool {
        self.find_table(schema, name).is_some()
            || self.find_sequence(schema, name).is_some()
            || self.find_index(schema, name).is_some()
    }

    pub fn find_index(&self, schema: u32, name: &str) -> Option<(u32, &Index)> {
        self.tables
            .values()
            .filter(|t| t.schema == schema)
            .find_map(|t| t.indexes.iter().find(|i| i.name == name).map(|i| (t.oid, i)))
    }

    /// Name of the relation with `oid` (table, sequence or index).
    pub fn relation_name(&self, oid: u32) -> Option<(u32, String)> {
        if let Some(t) = self.tables.get(&oid) {
            return Some((t.schema, t.name.clone()));
        }
        if let Some(s) = self.sequences.get(&oid) {
            return Some((s.schema, s.name.clone()));
        }
        for t in self.tables.values() {
            if let Some(i) = t.indexes.iter().find(|i| i.oid == oid) {
                return Some((t.schema, i.name.clone()));
            }
        }
        None
    }

    /// A relation name not already taken in `schema`, Postgres style
    /// (`base`, `base1`, `base2`...).
    pub fn unique_rel_name(&self, schema: u32, base: &str) -> String {
        let base = choose_name(base);
        if !self.relation_exists(schema, &base) {
            return base;
        }
        for i in 1.. {
            let cand = choose_name_suffixed(&base, &i.to_string());
            if !self.relation_exists(schema, &cand) {
                return cand;
            }
        }
        unreachable!()
    }

    pub fn constraint_name_taken(&self, table: u32, name: &str) -> bool {
        self.table(table).is_some_and(|t| t.constraints.iter().any(|c| c.name == name))
    }
}

/// Truncates to 63 bytes like Postgres's makeObjectName.
pub fn choose_name(s: &str) -> String {
    super::types::truncate_name(s)
}

fn choose_name_suffixed(base: &str, suffix: &str) -> String {
    let mut b = base.to_string();
    while b.len() + suffix.len() > 63 {
        b.pop();
    }
    format!("{b}{suffix}")
}

/// `makeObjectName(table, column, label)`: `t_col_label`, truncating the
/// longer of table/column to fit 63 bytes.
pub fn make_object_name(name1: &str, name2: Option<&str>, label: &str) -> String {
    let mut n1 = name1.to_string();
    let mut n2 = name2.unwrap_or("").to_string();
    let overhead = label.len() + 1 + if name2.is_some() { 1 } else { 0 };
    while n1.len() + n2.len() + overhead > 63 {
        if n1.len() > n2.len() {
            n1.pop();
        } else {
            n2.pop();
        }
    }
    match name2 {
        Some(_) => format!("{n1}_{n2}_{label}"),
        None => format!("{n1}_{label}"),
    }
}

pub fn undefined_table(name: &str) -> PgError {
    PgError::new(code::UNDEFINED_TABLE, format!("relation \"{name}\" does not exist"))
}

pub fn check_unique_violation(
    t: &Table,
    cols: &[usize],
    row: &Row,
    skip: Option<usize>,
    nulls_not_distinct: bool,
) -> Option<usize> {
    if !nulls_not_distinct && cols.iter().any(|&c| row[c].is_null()) {
        return None;
    }
    t.rows.iter().enumerate().position(|(i, r)| {
        Some(i) != skip && cols.iter().all(|&c| super::types::values_equal(&r[c], &row[c]))
    })
}

pub fn ensure(cond: bool, err: impl FnOnce() -> PgError) -> PgResult<()> {
    if cond { Ok(()) } else { Err(err()) }
}
