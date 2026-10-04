use crate::mysql::catalog::{Column, ColumnType, UniqueKey};
use crate::mysql::error::MySqlError;
use crate::mysql::plan::{
    self as plan, AggFunc, AlterOp, ArithOp, CmpOp, ColumnPos, Expr, InsertMode, JoinOp, Plan,
    SetOpKind, SortKey, contains_agg,
};
use crate::mysql::types::Value;
use sqlparser::ast::{
    Assignment, BinaryOperator, ColumnDef, DataType, Expr as AstExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, OrderByKind, OrderBySort, Query, SelectItem, SetExpr, SetOperator, SetQuantifier,
    Statement, TableConstraint, TableFactor, TableWithJoins, UnaryOperator, Value as AstValue,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Binder {
    pub current_db: Option<String>,
    pub prepared_types: HashMap<String, Vec<Value>>,
    /// Assigns each `?` placeholder encountered while binding an
    /// expression its positional index (0, 1, 2, ...), in left-to-right
    /// order — matching how `COM_STMT_EXECUTE` lays out bound parameter
    /// values on the wire.
    param_counter: usize,
    /// Every `?`'s (line, column) in the statement's text, in textual
    /// order. A placeholder's parameter index is its rank here, not the
    /// order the binder happens to visit it in -- found via testing before
    /// a public release: the SELECT list is bound before WHERE but HAVING
    /// after GROUP BY, so `SELECT ? ... WHERE id = ?` swapped its
    /// parameters and silently matched the wrong rows.
    placeholder_at: Vec<(u64, u64)>,
    /// The statement's text (see `with_sql`), for labeling result columns.
    sql: String,
    /// `WITH` queries in scope, innermost last: each name with the plan a
    /// reference to it binds to (`Plan::Derived`, or `Plan::CteRef` inside
    /// a recursive CTE's own step).
    ctes: Vec<(String, Plan)>,
    /// Set when a `Plan::CteRef` is bound, to tell a recursive CTE's step
    /// that refers to itself from one that doesn't.
    cte_ref_used: bool,
    /// Whether a window function is allowed where binding is now (the
    /// select list and ORDER BY; MySQL rejects it elsewhere with 3593).
    window_ok: bool,
    /// The current SELECT's `WINDOW name AS (...)` definitions.
    named_windows: Vec<sqlparser::ast::NamedWindowDefinition>,
    /// Gives each window function in the statement its own id.
    window_counter: usize,
}

impl Binder {
    pub fn new(current_db: Option<String>) -> Self {
        Self {
            current_db,
            prepared_types: HashMap::new(),
            param_counter: 0,
            placeholder_at: Vec::new(),
            sql: String::new(),
            ctes: Vec::new(),
            cte_ref_used: false,
            window_ok: false,
            named_windows: Vec::new(),
            window_counter: 0,
        }
    }

    /// How many `?` placeholders the statement's text has (0 without
    /// `with_sql`) -- the parameter count a prepared statement reports.
    pub fn placeholder_count(&self) -> usize {
        self.placeholder_at.len()
    }

    /// Numbers placeholders by their position in `sql` (see
    /// `placeholder_at`). Without it, they're numbered in binding order.
    pub fn with_sql(mut self, sql: &str) -> Self {
        use sqlparser::tokenizer::{Token, Tokenizer};
        let dialect = sqlparser::dialect::MySqlDialect {};
        if let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize_with_location() {
            self.placeholder_at = tokens
                .iter()
                .filter(|t| matches!(t.token, Token::Placeholder(_)))
                .map(|t| (t.span.start.line, t.span.start.column))
                .collect();
        }
        self.sql = sql.to_string();
        self
    }

    /// An expression's text exactly as written in the statement: from its
    /// parse span's start (1-based line/column, in characters) to the end
    /// of the select item -- the next top-level `,` or clause keyword,
    /// skipping over parentheses and quoted strings. (The span's own end
    /// stops short of a function call's closing parenthesis.)
    fn source_text(&self, e: &AstExpr) -> Option<String> {
        use sqlparser::ast::Spanned;
        if let AstExpr::Value(sqlparser::ast::ValueWithSpan {
            value: AstValue::SingleQuotedString(s) | AstValue::DoubleQuotedString(s),
            ..
        }) = e
        {
            return Some(s.clone()); // MySQL labels a string literal by its value
        }
        if let AstExpr::UnaryOp { op: UnaryOperator::Minus, expr } = e {
            return self.source_text(expr).map(|t| format!("-{t}"));
        }
        let span = e.span();
        if self.sql.is_empty() || span.start.line == 0 {
            return None;
        }
        let mut start = 0usize;
        for (i, l) in self.sql.split('\n').enumerate() {
            if i + 1 == span.start.line as usize {
                start += (span.start.column as usize).saturating_sub(1);
                break;
            }
            start += l.chars().count() + 1;
        }
        let chars: Vec<char> = self.sql.chars().collect();
        let (mut depth, mut quote, mut end) = (0i32, None::<char>, chars.len());
        let mut k = start;
        while k < chars.len() {
            let c = chars[k];
            match quote {
                Some(q) if c == q => quote = None,
                Some(_) => {}
                None => match c {
                    '\'' | '"' | '`' => quote = Some(c),
                    '(' => depth += 1,
                    ')' if depth == 0 => {
                        end = k;
                        break;
                    }
                    ')' => depth -= 1,
                    ',' | ';' if depth == 0 => {
                        end = k;
                        break;
                    }
                    c if depth == 0 && c.is_whitespace() => {
                        let rest: String =
                            chars[k..].iter().take(12).collect::<String>().to_ascii_uppercase();
                        let rest = rest.trim_start();
                        if [
                            "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "UNION", "INTO",
                            "FOR", "WINDOW",
                        ]
                        .iter()
                        .any(|kw| {
                            rest.starts_with(kw)
                                && !rest[kw.len()..]
                                    .starts_with(|c: char| c.is_alphanumeric() || c == '_')
                        }) {
                            end = k;
                            break;
                        }
                    }
                    _ => {}
                },
            }
            k += 1;
        }
        let text: String = chars.get(start..end)?.iter().collect();
        let text = text.trim();
        (!text.is_empty()).then(|| text.to_string())
    }

    pub fn bind_statement(&mut self, stmt: Statement) -> Result<Plan, MySqlError> {
        match stmt {
            Statement::Query(query) => self.bind_query(*query),
            Statement::ShowDatabases { .. } => Ok(Plan::ShowDatabases),
            Statement::ShowTables { full, show_options, .. } => {
                let db = match &show_options.show_in {
                    Some(sqlparser::ast::ShowStatementIn { parent_name: Some(name), .. }) => {
                        name.to_string().trim_matches('`').to_string()
                    }
                    _ => self
                        .current_db
                        .clone()
                        .ok_or_else(|| MySqlError::new(1046, "3D000", "No database selected"))?,
                };
                // Found via testing before a public release: the LIKE
                // pattern used to be ignored, listing every table.
                let like = match &show_options.filter_position {
                    Some(
                        sqlparser::ast::ShowStatementFilterPosition::Suffix(f)
                        | sqlparser::ast::ShowStatementFilterPosition::Infix(f),
                    ) => match f {
                        sqlparser::ast::ShowStatementFilter::Like(p)
                        | sqlparser::ast::ShowStatementFilter::ILike(p) => Some(p.clone()),
                        _ => return Err(MySqlError::unsupported("SHOW TABLES WHERE")),
                    },
                    None => None,
                };
                Ok(Plan::ShowTables { db, like, full })
            }
            Statement::ShowColumns { show_options, .. } => {
                let table_name_str = match show_options.show_in {
                    Some(sqlparser::ast::ShowStatementIn { parent_name: Some(name), .. }) => {
                        let parts: Vec<String> = name
                            .0
                            .iter()
                            .filter_map(|n| match n {
                                sqlparser::ast::ObjectNamePart::Identifier(id) => {
                                    Some(id.value.clone())
                                }
                                _ => None,
                            })
                            .collect();
                        parts.join(".")
                    }
                    _ => return Err(MySqlError::unsupported("SHOW COLUMNS without IN table")),
                };
                let obj = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    sqlparser::ast::Ident::new(table_name_str),
                )]);
                let (db, table) = self.resolve_table_name(&obj)?;
                Ok(Plan::ShowColumns { db, table })
            }
            Statement::ShowCreate { obj_type, obj_name } => {
                if let sqlparser::ast::ShowCreateObject::Table = obj_type {
                    let (db, table) = self.resolve_table_name(&obj_name)?;
                    Ok(Plan::ShowCreateTable { db, table })
                } else {
                    Err(MySqlError::unsupported("SHOW CREATE object type"))
                }
            }
            Statement::Use(use_db) => match use_db {
                sqlparser::ast::Use::Object(db_name) => {
                    let name = db_name
                        .0
                        .iter()
                        .map(|n| match n {
                            sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                            _ => "".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(".");
                    Ok(Plan::Use(name))
                }
                _ => Err(MySqlError::unsupported("USE statement format")),
            },
            Statement::CreateTable(create_table) => {
                if create_table.query.is_some() || create_table.like.is_some() {
                    return Err(MySqlError::unsupported("CREATE TABLE ... SELECT/LIKE"));
                }
                let if_not_exists = create_table.if_not_exists;
                let mut plan = self.bind_create_table(
                    create_table.name,
                    create_table.columns,
                    create_table.constraints,
                )?;
                if let Plan::CreateTable { if_not_exists: f, .. } = &mut plan {
                    *f = if_not_exists;
                }
                Ok(plan)
            }
            Statement::AlterTable(alter) => self.bind_alter_table(alter),
            // Found via testing: DROP DATABASE was unsupported, which broke
            // test runners (Django's among them) that create and drop a
            // test database on every run.
            Statement::Drop {
                object_type:
                    sqlparser::ast::ObjectType::Database | sqlparser::ast::ObjectType::Schema,
                if_exists,
                names,
                ..
            } => {
                let name = names
                    .first()
                    .map(|n| n.to_string().trim_matches('`').to_string())
                    .ok_or_else(|| MySqlError::syntax_error("DROP DATABASE needs a name"))?;
                Ok(Plan::DropDatabase { name, if_exists })
            }
            Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                let tables =
                    names.iter().map(|n| self.resolve_table_name(n)).collect::<Result<_, _>>()?;
                Ok(Plan::DropTable { tables, if_exists })
            }
            Statement::Truncate(t) => {
                let target = t
                    .table_names
                    .first()
                    .ok_or_else(|| MySqlError::syntax_error("TRUNCATE needs a table"))?;
                let (db, table) = self.resolve_table_name(&target.name)?;
                Ok(Plan::Truncate { db, table })
            }
            // `DESCRIBE t` / `DESC t` / `EXPLAIN t` are SHOW COLUMNS.
            Statement::ExplainTable { table_name, .. } => {
                let (db, table) = self.resolve_table_name(&table_name)?;
                Ok(Plan::ShowColumns { db, table })
            }
            Statement::CreateDatabase { db_name, if_not_exists, .. } => {
                let name = db_name
                    .0
                    .iter()
                    .filter_map(|p| match p {
                        sqlparser::ast::ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(".");
                Ok(Plan::CreateDatabase { name, if_not_exists })
            }
            Statement::CreateIndex(ci) => {
                let (db, table) = self.resolve_table_name(&ci.table_name)?;
                let columns = ci
                    .columns
                    .into_iter()
                    .filter_map(|c| match c.column.expr {
                        AstExpr::Identifier(id) => Some(id.value),
                        _ => None,
                    })
                    .collect();
                let unique = ci.unique.then(|| {
                    ci.name
                        .as_ref()
                        .map(|n| n.to_string().trim_matches('`').to_string())
                        .unwrap_or_default()
                });
                Ok(Plan::CreateIndex {
                    db,
                    table,
                    columns,
                    if_not_exists: ci.if_not_exists,
                    unique,
                })
            }
            Statement::Insert(insert) => self.bind_insert(insert),
            Statement::Update(update) => self.bind_update(
                update.table,
                update.assignments,
                update.selection,
                update.order_by,
                update.limit,
            ),
            Statement::Delete(delete) => {
                let from = match delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(f) => f,
                    sqlparser::ast::FromTable::WithoutKeyword(f) => f,
                };
                self.bind_delete(from, delete.selection, delete.order_by, delete.limit)
            }
            _ => Err(MySqlError::unsupported("statement")),
        }
    }

    /// One column definition (CREATE TABLE, ALTER TABLE ADD/MODIFY/CHANGE).
    /// A column-level `UNIQUE` is pushed onto `unique_keys`.
    fn bind_column_def(
        &mut self,
        col_def: ColumnDef,
        unique_keys: &mut Vec<UniqueKey>,
    ) -> Result<Column, MySqlError> {
        let col_name = col_def.name.value.clone();
        let col_type = match &col_def.data_type {
            // Every integer type keeps its own range (and UNSIGNED), so an
            // out-of-range value is rejected or clamped exactly as MySQL
            // would. Found via a differential test against real MySQL: they
            // all used to be one unbounded integer.
            DataType::Int(_)
            | DataType::Integer(_)
            | DataType::IntUnsigned(_)
            | DataType::IntegerUnsigned(_) => ColumnType::Int,
            DataType::TinyInt(_) | DataType::TinyIntUnsigned(_) | DataType::UTinyInt => {
                ColumnType::TinyInt
            }
            DataType::SmallInt(_) | DataType::SmallIntUnsigned(_) => ColumnType::SmallInt,
            DataType::MediumInt(_) | DataType::MediumIntUnsigned(_) => ColumnType::MediumInt,
            DataType::BigInt(_) | DataType::BigIntUnsigned(_) => ColumnType::BigInt,
            DataType::Varchar(len) => {
                let l = len
                    .as_ref()
                    .and_then(|e| match &e {
                        sqlparser::ast::CharacterLength::IntegerLength { length, .. } => {
                            Some(*length as usize)
                        }
                        _ => None,
                    })
                    .unwrap_or(255);
                ColumnType::Varchar(l)
            }
            DataType::Char(len) | DataType::Character(len) | DataType::Nvarchar(len) => {
                ColumnType::Varchar(match len {
                    Some(sqlparser::ast::CharacterLength::IntegerLength { length, .. }) => {
                        *length as usize
                    }
                    _ => 1,
                })
            }
            DataType::Text | DataType::TinyText | DataType::MediumText | DataType::LongText => {
                ColumnType::Text
            }
            DataType::Float(_) | DataType::Float4 | DataType::Real => ColumnType::Float,
            DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 => {
                ColumnType::Double
            }
            DataType::Bool => ColumnType::Boolean,
            DataType::JSON => ColumnType::Json,
            DataType::Enum(members, _) => ColumnType::Enum(
                members
                    .iter()
                    .map(|m| match m {
                        sqlparser::ast::EnumMember::Name(n)
                        | sqlparser::ast::EnumMember::NamedValue(n, _) => n.clone(),
                    })
                    .collect(),
            ),
            DataType::Blob(_)
            | DataType::TinyBlob
            | DataType::MediumBlob
            | DataType::LongBlob
            | DataType::Binary(_)
            | DataType::Varbinary(_) => ColumnType::Blob,
            DataType::Decimal(exact) | DataType::Numeric(exact) | DataType::Dec(exact) => {
                let (p, s) = match exact {
                    sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                        (*p as u8, *s as u8)
                    }
                    sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                    sqlparser::ast::ExactNumberInfo::None => (10, 0),
                };
                ColumnType::Decimal(p, s)
            }
            DataType::Date => ColumnType::Date,
            // `TIMESTAMP` is stored and returned like `DATETIME` -- this
            // engine has no session time zone for its UTC-conversion
            // semantics to differ by.
            DataType::Datetime(_) | DataType::Timestamp(_, _) => ColumnType::Datetime,
            DataType::Boolean => ColumnType::Boolean,
            _ => {
                return Err(MySqlError::unsupported(&format!("data type {:?}", col_def.data_type)));
            }
        };

        let mut not_null = false;
        let mut auto_increment = false;
        let mut primary_key = false;
        let mut default = None;
        let mut default_now = false;
        let mut on_update_now = false;

        for opt in &col_def.options {
            match &opt.option {
                sqlparser::ast::ColumnOption::NotNull => not_null = true,
                sqlparser::ast::ColumnOption::Unique(_) => {
                    unique_keys.push(UniqueKey {
                        name: col_name.clone(),
                        columns: vec![col_name.clone()],
                    });
                }
                sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                    primary_key = true;
                    not_null = true;
                }
                sqlparser::ast::ColumnOption::DialectSpecific(tokens) => {
                    let text = tokens.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(" ");
                    if text.eq_ignore_ascii_case("auto_increment") {
                        auto_increment = true;
                    }
                }
                // A default that's a literal (the overwhelmingly common
                // case: `DEFAULT 0`, `DEFAULT ''`, `DEFAULT NULL`,
                // including WordPress's own schema's `DEFAULT '0'` on
                // NOT NULL columns like comment_count) binds to a
                // `Const`, which is stored directly. A non-constant
                // default (`DEFAULT CURRENT_TIMESTAMP`, an expression)
                // isn't evaluated here -- falls back to no default,
                // same as before this existed at all.
                sqlparser::ast::ColumnOption::Default(expr) => {
                    // MySQL 8's expression-default syntax wraps it in
                    // parentheses: `DEFAULT (now())`, what SQLAlchemy
                    // emits for `server_default=func.now()`.
                    let mut expr = expr;
                    while let AstExpr::Nested(inner) = expr {
                        expr = inner;
                    }
                    if is_current_time(expr) {
                        default_now = true;
                    } else if let Ok(Expr::Const(v)) = self.bind_expr(expr.clone()) {
                        default = Some(v);
                    } else {
                        // Found via testing before a public release: a
                        // non-constant default used to be dropped
                        // silently, so a `NOT NULL DEFAULT <expr>`
                        // column then rejected every insert that relied
                        // on it. Refuse it at CREATE time instead.
                        return Err(MySqlError::unsupported(&format!("DEFAULT expression {expr}")));
                    }
                }
                sqlparser::ast::ColumnOption::OnUpdate(expr)
                    if is_current_time(match expr {
                        AstExpr::Nested(inner) => inner,
                        e => e,
                    }) =>
                {
                    on_update_now = true;
                }
                _ => {}
            }
        }

        Ok(Column {
            name: col_name,
            ty: col_type,
            not_null,
            default,
            auto_increment,
            primary_key,
            default_now,
            on_update_now,
            unsigned: matches!(
                col_def.data_type,
                DataType::IntUnsigned(_)
                    | DataType::IntegerUnsigned(_)
                    | DataType::TinyIntUnsigned(_)
                    | DataType::UTinyInt
                    | DataType::SmallIntUnsigned(_)
                    | DataType::MediumIntUnsigned(_)
                    | DataType::BigIntUnsigned(_)
            ),
        })
    }

    /// `ALTER TABLE`, the forms migration tools emit (Django, Rails,
    /// Laravel, Alembic). Found via testing before a public release: any
    /// ALTER failed, so no ORM's migrations could run.
    fn bind_alter_table(&mut self, alter: sqlparser::ast::AlterTable) -> Result<Plan, MySqlError> {
        use sqlparser::ast::{AlterColumnOperation as Acol, AlterTableOperation as A};
        let (db, table) = self.resolve_table_name(&alter.name)?;
        let pos = |p: Option<sqlparser::ast::MySQLColumnPosition>| {
            p.map(|p| match p {
                sqlparser::ast::MySQLColumnPosition::First => ColumnPos::First,
                sqlparser::ast::MySQLColumnPosition::After(i) => ColumnPos::After(i.value),
            })
        };
        let idents = |cols: &[sqlparser::ast::IndexColumn]| -> Vec<String> {
            cols.iter()
                .filter_map(|c| match &c.column.expr {
                    AstExpr::Identifier(i) => Some(i.value.clone()),
                    _ => None,
                })
                .collect()
        };
        let mut ops = Vec::new();
        for op in alter.operations {
            let next = match op {
                A::AddColumn { column_def, column_position, if_not_exists, .. } => {
                    let mut unique = Vec::new();
                    let col = self.bind_column_def(column_def, &mut unique)?;
                    AlterOp::AddColumn { col, unique, pos: pos(column_position), if_not_exists }
                }
                A::DropColumn { column_names, if_exists, .. } => {
                    for c in column_names {
                        ops.push(AlterOp::DropColumn { name: c.value, if_exists });
                    }
                    continue;
                }
                A::ModifyColumn { col_name, data_type, options, column_position } => {
                    let mut unique = Vec::new();
                    let def = ColumnDef {
                        name: col_name.clone(),
                        data_type,
                        options: options_with_names(options),
                    };
                    let col = self.bind_column_def(def, &mut unique)?;
                    AlterOp::ReplaceColumn {
                        old: col_name.value,
                        col,
                        unique,
                        pos: pos(column_position),
                    }
                }
                A::ChangeColumn { old_name, new_name, data_type, options, column_position } => {
                    let mut unique = Vec::new();
                    let def = ColumnDef {
                        name: new_name,
                        data_type,
                        options: options_with_names(options),
                    };
                    let col = self.bind_column_def(def, &mut unique)?;
                    AlterOp::ReplaceColumn {
                        old: old_name.value,
                        col,
                        unique,
                        pos: pos(column_position),
                    }
                }
                A::RenameColumn { old_column_name, new_column_name } => {
                    AlterOp::RenameColumn { old: old_column_name.value, new: new_column_name.value }
                }
                A::RenameTable { table_name } => {
                    let name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(n)
                        | sqlparser::ast::RenameTableNameKind::To(n) => n,
                    };
                    AlterOp::RenameTable(self.resolve_table_name(&name)?.1)
                }
                A::AddConstraint { constraint, .. } => match constraint {
                    TableConstraint::Unique(u) => {
                        let columns = idents(&u.columns);
                        let name = u
                            .name
                            .as_ref()
                            .or(u.index_name.as_ref())
                            .map(|i| i.value.clone())
                            .unwrap_or_else(|| columns.first().cloned().unwrap_or_default());
                        AlterOp::AddUnique(UniqueKey { name, columns })
                    }
                    TableConstraint::PrimaryKey(pk) => AlterOp::AddPrimaryKey(idents(&pk.columns)),
                    TableConstraint::ForeignKey(fk) => {
                        AlterOp::AddForeignKey(self.bind_foreign_key(&fk)?)
                    }
                    // INDEX, CHECK: accepted, not enforced.
                    _ => AlterOp::Noop,
                },
                A::DropIndex { name } | A::DropConstraint { name, .. } => {
                    AlterOp::DropKey(name.value)
                }
                A::DropForeignKey { name, .. } => AlterOp::DropForeignKey(name.value),
                A::DropPrimaryKey { .. } => AlterOp::DropPrimaryKey,
                A::AlterColumn { column_name, op } => match op {
                    Acol::SetDefault { value } => {
                        let mut v = &value;
                        while let AstExpr::Nested(inner) = v {
                            v = inner;
                        }
                        if is_current_time(v) {
                            AlterOp::SetDefault { col: column_name.value, default: None, now: true }
                        } else if let Ok(Expr::Const(c)) = self.bind_expr(v.clone()) {
                            AlterOp::SetDefault {
                                col: column_name.value,
                                default: Some(c),
                                now: false,
                            }
                        } else {
                            return Err(MySqlError::unsupported(&format!(
                                "DEFAULT expression {value}"
                            )));
                        }
                    }
                    Acol::DropDefault => {
                        AlterOp::SetDefault { col: column_name.value, default: None, now: false }
                    }
                    other => {
                        return Err(MySqlError::unsupported(&format!("ALTER COLUMN {other}")));
                    }
                },
                A::AutoIncrement { value, .. } => match &value.value {
                    AstValue::Number(n, _) => AlterOp::AutoIncrement(n.parse().unwrap_or(1)),
                    _ => AlterOp::Noop,
                },
                A::Algorithm { .. } | A::Lock { .. } => AlterOp::Noop,
                other => return Err(MySqlError::unsupported(&format!("ALTER TABLE {other}"))),
            };
            ops.push(next);
        }
        Ok(Plan::AlterTable { db, table, ops })
    }

    fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Result<Plan, MySqlError> {
        let (db, table) = self.resolve_table_name(&name)?;
        let mut cols = Vec::new();
        let mut unique_keys: Vec<UniqueKey> = Vec::new();

        for col_def in columns {
            let col = self.bind_column_def(col_def, &mut unique_keys)?;
            cols.push(col);
        }

        // A table-level `PRIMARY KEY (...)` clause (WordPress-style schemas
        // always define it this way, never as a column option) was
        // previously discarded entirely rather than just its enforcement --
        // mark the referenced column(s) as the primary key so that metadata
        // isn't lost, even though (like column-level `UNIQUE`, already a
        // no-op above) uniqueness itself still isn't enforced anywhere in
        // this engine. Other table-level constraint kinds (`UNIQUE`,
        // `FOREIGN KEY`, `KEY`/`INDEX`, `FULLTEXT`/`SPATIAL`) are accepted
        // but not tracked, for the same reason.
        for constraint in &constraints {
            if let TableConstraint::Unique(u) = constraint {
                let columns: Vec<String> = u
                    .columns
                    .iter()
                    .filter_map(|c| match &c.column.expr {
                        AstExpr::Identifier(i) => Some(i.value.clone()),
                        _ => None,
                    })
                    .collect();
                if !columns.is_empty() {
                    let name = u
                        .name
                        .as_ref()
                        .or(u.index_name.as_ref())
                        .map(|i| i.value.clone())
                        .unwrap_or_else(|| columns[0].clone());
                    unique_keys.push(UniqueKey { name, columns });
                }
            }
            if let TableConstraint::PrimaryKey(pk) = constraint {
                for idx_col in &pk.columns {
                    if let AstExpr::Identifier(ident) = &idx_col.column.expr {
                        for col in cols.iter_mut() {
                            if col.name == ident.value {
                                col.primary_key = true;
                                col.not_null = true;
                            }
                        }
                    }
                }
            }
        }

        // Table-level FOREIGN KEY clauses (an inline column `REFERENCES` is
        // parsed and ignored, as in MySQL).
        let mut foreign_keys = Vec::new();
        for constraint in &constraints {
            if let TableConstraint::ForeignKey(fk) = constraint {
                foreign_keys.push(self.bind_foreign_key(fk)?);
            }
        }

        Ok(Plan::CreateTable {
            db,
            table,
            columns: cols,
            unique_keys,
            foreign_keys,
            if_not_exists: false,
        })
    }

    fn bind_insert(&mut self, insert: sqlparser::ast::Insert) -> Result<Plan, MySqlError> {
        let table_name = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => name,
            _ => return Err(MySqlError::unsupported("insert table target")),
        };
        let (db, table) = self.resolve_table_name(table_name)?;
        let mut columns: Vec<String> = insert
            .columns
            .iter()
            .filter_map(|name| match name.0.last() {
                Some(sqlparser::ast::ObjectNamePart::Identifier(id)) => Some(id.value.clone()),
                _ => None,
            })
            .collect();
        let mut rows = Vec::new();
        let mut select = None;

        if let Some(source) = insert.source {
            if let SetExpr::Values(values) = *source.body {
                for row in values.rows {
                    let mut r = Vec::new();
                    for expr in row.content {
                        r.push(self.bind_expr(expr)?);
                    }
                    rows.push(r);
                }
            } else {
                select = Some(self.bind_query(*source)?);
            }
        } else if !insert.assignments.is_empty() {
            // `INSERT INTO t SET a = 1, b = 2` -- found via testing before a
            // public release: this form used to insert nothing at all (no
            // rows, no error).
            let mut row = Vec::new();
            for a in insert.assignments {
                columns.push(assignment_column(&a.target)?);
                row.push(self.bind_expr(a.value)?);
            }
            rows.push(row);
        }

        // Found via testing before a public release: IGNORE / REPLACE /
        // ON DUPLICATE KEY UPDATE used to be dropped silently, so an upsert
        // (WordPress's own `add_option()` is one) inserted a duplicate row.
        let mode = if insert.replace_into {
            InsertMode::Replace
        } else if let Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) = insert.on {
            // MySQL 8.0.19+ `INSERT ... AS new ON DUPLICATE KEY UPDATE c = new.c`:
            // `new.c` means the same as `VALUES(c)`.
            let row_alias = insert.insert_alias.as_ref().map(|a| a.row_alias.to_string());
            let mut out = Vec::new();
            for a in assignments {
                let col = assignment_column(&a.target)?;
                let mut e = self.bind_expr(a.value)?;
                if let Some(alias) = &row_alias {
                    e = row_alias_to_values(e, alias);
                }
                out.push((col, e));
            }
            InsertMode::Upsert(out)
        } else if insert.ignore {
            InsertMode::Ignore
        } else if insert.on.is_some() {
            return Err(MySqlError::unsupported("INSERT ... ON CONFLICT"));
        } else {
            InsertMode::Error
        };

        let insert = Plan::Insert { db, table, columns, rows, mode };
        Ok(match select {
            Some(query) => Plan::InsertSelect { insert: Box::new(insert), query: Box::new(query) },
            None => insert,
        })
    }

    fn bind_update(
        &mut self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<AstExpr>,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        limit: Option<AstExpr>,
    ) -> Result<Plan, MySqlError> {
        if !table.joins.is_empty() {
            return Err(MySqlError::unsupported("multi-table UPDATE"));
        }
        let (db, table_name, alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => {
                let (db, t) = self.resolve_table_name(name)?;
                (db, t, alias.as_ref().map(|a| a.name.value.clone()))
            }
            _ => return Err(MySqlError::unsupported("update target")),
        };
        let alias = alias.as_deref();

        let mut out_assignments = Vec::new();
        for a in assignments {
            let col_name = assignment_column(&a.target)?;
            let expr = strip_alias(self.bind_expr(a.value)?, alias);
            out_assignments.push((col_name, expr));
        }

        let sel = match selection {
            Some(expr) => Some(strip_alias(self.bind_expr(expr)?, alias)),
            None => None,
        };
        let (order, limit) = self.bind_dml_order_limit(order_by, limit)?;
        let order = order.into_iter().map(|(e, asc)| (strip_alias(e, alias), asc)).collect();

        Ok(Plan::Update {
            db,
            table: table_name,
            assignments: out_assignments,
            selection: sel,
            order,
            limit,
        })
    }

    fn bind_delete(
        &mut self,
        from: Vec<TableWithJoins>,
        selection: Option<AstExpr>,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        limit: Option<AstExpr>,
    ) -> Result<Plan, MySqlError> {
        if from.len() != 1 || !from[0].joins.is_empty() {
            return Err(MySqlError::unsupported("multi-table DELETE"));
        }
        let (db, table_name, alias) = match &from[0].relation {
            TableFactor::Table { name, alias, .. } => {
                let (db, t) = self.resolve_table_name(name)?;
                (db, t, alias.as_ref().map(|a| a.name.value.clone()))
            }
            _ => return Err(MySqlError::unsupported("delete target")),
        };
        let alias = alias.as_deref();

        let sel = match selection {
            Some(expr) => Some(strip_alias(self.bind_expr(expr)?, alias)),
            None => None,
        };
        let (order, limit) = self.bind_dml_order_limit(order_by, limit)?;
        let order = order.into_iter().map(|(e, asc)| (strip_alias(e, alias), asc)).collect();

        Ok(Plan::Delete { db, table: table_name, selection: sel, order, limit })
    }

    /// `UPDATE`/`DELETE ... ORDER BY ... LIMIT n`. Found via testing before a
    /// public release: both used to be ignored silently, so `DELETE FROM t
    /// WHERE ... ORDER BY id LIMIT 1` deleted every matching row.
    fn bind_dml_order_limit(
        &mut self,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        limit: Option<AstExpr>,
    ) -> Result<(OrderKeys, Option<Expr>), MySqlError> {
        let mut order = Vec::new();
        for item in order_by {
            let asc = !matches!(item.options.sort, Some(OrderBySort::Desc));
            order.push((self.bind_expr(item.expr)?, asc));
        }
        let limit = limit.map(|e| self.bind_count(e)).transpose()?;
        Ok((order, limit))
    }

    fn bind_query(&mut self, query: Query) -> Result<Plan, MySqlError> {
        // A subquery is its own scope for window functions and names.
        let saved_ok = std::mem::replace(&mut self.window_ok, false);
        let saved_named = std::mem::take(&mut self.named_windows);
        let res = self.bind_query_scoped(query);
        self.window_ok = saved_ok;
        self.named_windows = saved_named;
        res
    }

    fn bind_query_scoped(&mut self, query: Query) -> Result<Plan, MySqlError> {
        let scope = self.ctes.len();
        let res = (|| {
            if let Some(with) = query.with {
                for cte in with.cte_tables {
                    self.bind_cte(cte, with.recursive)?;
                }
            }
            self.bind_body(*query.body, query.order_by, query.limit_clause)
        })();
        self.ctes.truncate(scope);
        res
    }

    /// Puts one `WITH` query in scope for the rest of the statement.
    fn bind_cte(&mut self, cte: sqlparser::ast::Cte, recursive: bool) -> Result<(), MySqlError> {
        let name = cte.alias.name.value.clone();
        let columns: Vec<String> = cte.alias.columns.iter().map(|c| c.name.value.clone()).collect();
        let query = *cte.query;
        if recursive
            && query.order_by.is_none()
            && query.limit_clause.is_none()
            && let SetExpr::SetOperation { op: SetOperator::Union, set_quantifier, left, right } =
                &*query.body
        {
            let all = matches!(set_quantifier, SetQuantifier::All);
            let anchor = self.bind_body((**left).clone(), None, None)?;
            let columns = if columns.is_empty() {
                plan::static_names(&anchor).ok_or_else(|| {
                    MySqlError::unsupported("SELECT * in a recursive CTE without a column list")
                })?
            } else {
                columns
            };
            self.ctes.push((
                name.clone(),
                Plan::CteRef { name: name.clone(), alias: name.clone(), columns: columns.clone() },
            ));
            let used_before = std::mem::replace(&mut self.cte_ref_used, false);
            let step = self.bind_body((**right).clone(), None, None);
            let refers_to_itself = self.cte_ref_used;
            self.cte_ref_used = used_before;
            self.ctes.pop();
            let step = step?;
            let plan = if refers_to_itself {
                Plan::RecursiveCte {
                    name: name.clone(),
                    columns: columns.clone(),
                    anchor: Box::new(anchor),
                    step: Box::new(step),
                    all,
                }
            } else {
                Plan::SetOp {
                    op: SetOpKind::Union,
                    all,
                    left: Box::new(anchor),
                    right: Box::new(step),
                }
            };
            self.ctes
                .push((name.clone(), Plan::Derived { plan: Box::new(plan), alias: name, columns }));
            return Ok(());
        }
        let plan = self.bind_query(query)?;
        self.ctes
            .push((name.clone(), Plan::Derived { plan: Box::new(plan), alias: name, columns }));
        Ok(())
    }

    fn bind_limit(
        &mut self,
        limit_clause: Option<LimitClause>,
    ) -> Result<(Option<Expr>, Option<Expr>), MySqlError> {
        match limit_clause {
            Some(LimitClause::LimitOffset { limit, offset, limit_by }) => {
                if !limit_by.is_empty() {
                    return Err(MySqlError::unsupported("LIMIT BY"));
                }
                let limit = limit.map(|e| self.bind_count(e)).transpose()?;
                let offset = offset.map(|o| self.bind_count(o.value)).transpose()?;
                Ok((limit, offset))
            }
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                // `LIMIT offset, count`: bind in textual order.
                let offset = self.bind_count(offset)?;
                Ok((Some(self.bind_count(limit)?), Some(offset)))
            }
            None => Ok((None, None)),
        }
    }

    /// `ORDER BY`/`LIMIT` on a `UNION` (or a parenthesized query): sorts
    /// by output column, named or numbered.
    fn finish_set(
        &mut self,
        plan: Plan,
        order_by: Option<sqlparser::ast::OrderBy>,
        limit_clause: Option<LimitClause>,
    ) -> Result<Plan, MySqlError> {
        if order_by.is_none() && limit_clause.is_none() {
            return Ok(plan);
        }
        let names = plan::static_names(&plan);
        let mut order = Vec::new();
        if let Some(ob) = order_by {
            let OrderByKind::Expressions(items) = ob.kind else {
                return Err(MySqlError::unsupported("ORDER BY ALL"));
            };
            for item in items {
                let asc = !matches!(item.options.sort, Some(OrderBySort::Desc));
                let idx = match &item.expr {
                    AstExpr::Value(sqlparser::ast::ValueWithSpan {
                        value: AstValue::Number(n, _),
                        ..
                    }) => n.parse::<usize>().ok().filter(|n| *n > 0).map(|n| n - 1),
                    AstExpr::Identifier(i) => names
                        .as_ref()
                        .and_then(|ns| ns.iter().position(|n| n.eq_ignore_ascii_case(&i.value))),
                    AstExpr::CompoundIdentifier(ids) => names.as_ref().and_then(|ns| {
                        let last = &ids.last()?.value;
                        ns.iter().position(|n| n.eq_ignore_ascii_case(last))
                    }),
                    _ => return Err(MySqlError::unsupported("ORDER BY expression on a UNION")),
                };
                let Some(idx) = idx else {
                    return Err(MySqlError::new(
                        1054,
                        "42S22",
                        format!("Unknown column '{}' in 'order clause'", item.expr),
                    ));
                };
                order.push((SortKey::Output(idx), asc));
            }
        }
        let (limit, offset) = self.bind_limit(limit_clause)?;
        Ok(Plan::Finish {
            source: Box::new(plan),
            order,
            hidden: 0,
            distinct: false,
            limit,
            offset,
            calc_found_rows: false,
        })
    }

    fn bind_body(
        &mut self,
        body: SetExpr,
        order_by: Option<sqlparser::ast::OrderBy>,
        limit_clause: Option<LimitClause>,
    ) -> Result<Plan, MySqlError> {
        match body {
            SetExpr::Query(q) => {
                let plan = self.bind_query(*q)?;
                self.finish_set(plan, order_by, limit_clause)
            }
            SetExpr::SetOperation { op, set_quantifier, left, right } => {
                let kind = match op {
                    SetOperator::Union => SetOpKind::Union,
                    SetOperator::Intersect => SetOpKind::Intersect,
                    SetOperator::Except | SetOperator::Minus => SetOpKind::Except,
                };
                let all = match set_quantifier {
                    SetQuantifier::All => true,
                    SetQuantifier::Distinct | SetQuantifier::None => false,
                    _ => return Err(MySqlError::unsupported("set quantifier")),
                };
                let left = self.bind_body(*left, None, None)?;
                let right = self.bind_body(*right, None, None)?;
                let plan =
                    Plan::SetOp { op: kind, all, left: Box::new(left), right: Box::new(right) };
                self.finish_set(plan, order_by, limit_clause)
            }
            SetExpr::Select(select) => {
                let mut source =
                    if select.from.is_empty() { Plan::Dummy } else { self.bind_from(select.from)? };

                if let Some(selection) = select.selection {
                    let pred = self.bind_expr(selection)?;
                    source = Plan::Filter { source: Box::new(source), predicate: pred };
                }

                let distinct = match &select.distinct {
                    None => false,
                    Some(sqlparser::ast::Distinct::Distinct) => true,
                    Some(_) => return Err(MySqlError::unsupported("DISTINCT ON")),
                };

                // The projection is bound first: GROUP BY, HAVING and ORDER
                // BY may all refer to its aliases (`... AS total ORDER BY
                // total`) and positions (`ORDER BY 2`).
                let mut exprs = Vec::new();
                let mut names = Vec::new();
                // (alias, expr) for every `expr AS alias` item -- only real
                // aliases, not plain column names.
                let mut aliases: Vec<(String, Expr)> = Vec::new();
                let mut has_wildcard = false;
                self.named_windows = select.named_window.clone();
                self.window_ok = true;
                for item in select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            // Real MySQL labels an unaliased plain column
                            // reference with the column's own name, and
                            // anything else with the expression's source
                            // text exactly as written (`count(*)`, `1`,
                            // `price * 2`) -- what `row['COUNT(*)']`-style
                            // code reads. Found via testing before a public
                            // release: those used to be labeled `col0`.
                            let name = match &expr {
                                AstExpr::Identifier(ident) => ident.value.clone(),
                                AstExpr::CompoundIdentifier(idents) => idents
                                    .last()
                                    .map(|i| i.value.clone())
                                    .unwrap_or_else(|| "?".to_string()),
                                other => self.source_text(other).unwrap_or_else(|| "?".to_string()),
                            };
                            exprs.push(self.bind_expr(expr)?);
                            names.push(name);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let bound = self.bind_expr(expr)?;
                            aliases.push((alias.value.clone(), bound.clone()));
                            exprs.push(bound);
                            names.push(alias.value);
                        }
                        // `SELECT *` and `SELECT table.*` -- expanded to the
                        // real per-column values (and, in
                        // `plan::column_names`, the real per-column names)
                        // at execution time, not here -- the binder has no
                        // catalog access to look the table's columns up.
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                            exprs.push(Expr::Wildcard);
                            names.push("*".to_string());
                            has_wildcard = true;
                        }
                        _ => return Err(MySqlError::unsupported("select item")),
                    }
                }
                self.window_ok = false;
                let alias_of = |e: &AstExpr| -> Option<Expr> {
                    match e {
                        AstExpr::Identifier(i) => aliases
                            .iter()
                            .find(|(a, _)| a.eq_ignore_ascii_case(&i.value))
                            .map(|(_, x)| x.clone()),
                        _ => None,
                    }
                };
                let position_of = |e: &AstExpr| -> Option<usize> {
                    match e {
                        AstExpr::Value(sqlparser::ast::ValueWithSpan {
                            value: AstValue::Number(n, _),
                            ..
                        }) => n.parse::<usize>().ok(),
                        _ => None,
                    }
                };

                // GROUP BY accepts a column, an alias, or a position.
                let mut group_exprs: Vec<Expr> = Vec::new();
                match select.group_by {
                    GroupByExpr::Expressions(gexprs, _) => {
                        for g in gexprs {
                            if let Some(n) = position_of(&g) {
                                if n == 0 || n > exprs.len() || has_wildcard {
                                    return Err(MySqlError::new(
                                        1054,
                                        "42S22",
                                        format!("Unknown column '{n}' in 'group statement'"),
                                    ));
                                }
                                group_exprs.push(exprs[n - 1].clone());
                            } else if let Some(x) = alias_of(&g) {
                                group_exprs.push(x);
                            } else {
                                group_exprs.push(self.bind_expr(g)?);
                            }
                        }
                    }
                    GroupByExpr::All(_) => {
                        return Err(MySqlError::unsupported("GROUP BY ALL"));
                    }
                }

                // HAVING may name a SELECT-list alias (`HAVING cnt > 2`).
                let having = match select.having {
                    Some(h) => Some(substitute_aliases(self.bind_expr(h)?, &aliases)),
                    None => None,
                };

                // ORDER BY: a position addresses an output column directly;
                // an alias or any other expression is evaluated as a hidden
                // trailing column (see `Plan::Finish`).
                let mut order: Vec<(SortKey, bool)> = Vec::new();
                let mut hidden_exprs: Vec<Expr> = Vec::new();
                if let Some(ob) = order_by {
                    let items = match ob.kind {
                        OrderByKind::Expressions(items) => items,
                        OrderByKind::All(_) => return Err(MySqlError::unsupported("ORDER BY ALL")),
                    };
                    for item in items {
                        let asc = !matches!(item.options.sort, Some(OrderBySort::Desc));
                        if let Some(n) = position_of(&item.expr) {
                            if n == 0 || (!has_wildcard && n > exprs.len()) {
                                return Err(MySqlError::new(
                                    1054,
                                    "42S22",
                                    format!("Unknown column '{n}' in 'order clause'"),
                                ));
                            }
                            order.push((SortKey::Output(n - 1), asc));
                            continue;
                        }
                        let bound = match alias_of(&item.expr) {
                            Some(x) => x,
                            None => {
                                self.window_ok = true;
                                let b = self.bind_expr(item.expr);
                                self.window_ok = false;
                                b?
                            }
                        };
                        hidden_exprs.push(bound);
                        order.push((SortKey::Hidden(hidden_exprs.len() - 1), asc));
                    }
                }
                let (limit, offset) = match limit_clause {
                    Some(LimitClause::LimitOffset { limit, offset, limit_by }) => {
                        if !limit_by.is_empty() {
                            return Err(MySqlError::unsupported("LIMIT BY"));
                        }
                        let limit = limit.map(|e| self.bind_count(e)).transpose()?;
                        let offset = offset.map(|o| self.bind_count(o.value)).transpose()?;
                        (limit, offset)
                    }
                    Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                        // `LIMIT offset, count`: bind in textual order.
                        let offset = self.bind_count(offset)?;
                        (Some(self.bind_count(limit)?), Some(offset))
                    }
                    None => (None, None),
                };
                let calc_found_rows =
                    select.select_modifiers.as_ref().is_some_and(|m| m.sql_calc_found_rows);

                let is_aggregate = !group_exprs.is_empty()
                    || exprs.iter().any(contains_agg)
                    || hidden_exprs.iter().any(contains_agg)
                    || having.is_some();
                let hidden = hidden_exprs.len();
                exprs.extend(hidden_exprs);
                let plan = if is_aggregate {
                    Plan::Aggregate { source: Box::new(source), group_exprs, exprs, names, having }
                } else {
                    Plan::Project { source: Box::new(source), exprs, names }
                };
                if !order.is_empty()
                    || limit.is_some()
                    || offset.is_some()
                    || distinct
                    || calc_found_rows
                {
                    Ok(Plan::Finish {
                        source: Box::new(plan),
                        order,
                        hidden,
                        distinct,
                        limit,
                        offset,
                        calc_found_rows,
                    })
                } else {
                    Ok(plan)
                }
            }
            _ => Err(MySqlError::unsupported("query body")),
        }
    }

    fn bind_from(&mut self, from: Vec<TableWithJoins>) -> Result<Plan, MySqlError> {
        if from.len() != 1 {
            return Err(MySqlError::unsupported("multiple from clauses"));
        }

        let twj = &from[0];
        let mut plan = self.bind_table_factor(&twj.relation)?;

        for join in &twj.joins {
            let right = self.bind_table_factor(&join.relation)?;
            // Found via testing before a public release: a bare `JOIN`
            // (no `INNER`/`LEFT`/... keyword -- sqlparser's own generic
            // `JoinOperator::Join`, not `::Inner`) wasn't matched at all,
            // nor was a bare `LEFT JOIN` without the optional `OUTER`
            // keyword (`::Left`, a variant distinct from `::LeftOuter`) --
            // both are at least as common as the forms that were already
            // handled, if not more so.
            let (op, swap_sides) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinOp::Inner(self.bind_join_constraint(constraint)?), false)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinOp::Left(self.bind_join_constraint(constraint)?), false)
                }
                // `A RIGHT JOIN B ON cond` has no direct equivalent in
                // this engine's `JoinOp` (`Inner`/`Left`/`Cross` only) --
                // expressed instead as `B LEFT JOIN A ON cond`, same
                // condition, sides swapped. Values stay correct either
                // way since every column reference resolves by name, not
                // position; the one real difference is `SELECT *`'s
                // column order, which comes out as the right table's
                // columns before the left's rather than matching real
                // MySQL's left-then-right order -- a cosmetic gap, not a
                // correctness one.
                JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                    (JoinOp::Left(self.bind_join_constraint(constraint)?), true)
                }
                JoinOperator::CrossJoin(_) => (JoinOp::Cross, false),
                _ => return Err(MySqlError::unsupported("join operator")),
            };
            plan = if swap_sides {
                Plan::Join { left: Box::new(right), right: Box::new(plan), op }
            } else {
                Plan::Join { left: Box::new(plan), right: Box::new(right), op }
            };
        }

        Ok(plan)
    }

    fn bind_table_factor(&mut self, tf: &TableFactor) -> Result<Plan, MySqlError> {
        match tf {
            // `FROM DUAL` -- MySQL's one-row dummy table, needs no database
            // selected (connection pools ping with `SELECT 1 FROM DUAL`).
            TableFactor::Table { name, .. }
                if name.0.len() == 1
                    && matches!(&name.0[0], sqlparser::ast::ObjectNamePart::Identifier(id)
                        if id.value.eq_ignore_ascii_case("dual")) =>
            {
                Ok(Plan::Dummy)
            }
            TableFactor::Table { name, alias, .. }
                if let [sqlparser::ast::ObjectNamePart::Identifier(id)] = name.0.as_slice()
                    && let Some((_, plan)) =
                        self.ctes.iter().rev().find(|(n, _)| *n == id.value) =>
            {
                let mut plan = plan.clone();
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                match &mut plan {
                    Plan::Derived { alias: a, .. } | Plan::CteRef { alias: a, .. } => {
                        if let Some(x) = alias_name {
                            *a = x;
                        }
                    }
                    _ => {}
                }
                if matches!(plan, Plan::CteRef { .. }) {
                    self.cte_ref_used = true;
                }
                Ok(plan)
            }
            TableFactor::Derived { lateral, subquery, alias, .. } => {
                if *lateral {
                    return Err(MySqlError::unsupported("LATERAL"));
                }
                let Some(alias) = alias else {
                    return Err(MySqlError::new(
                        1248,
                        "42000",
                        "Every derived table must have its own alias",
                    ));
                };
                let plan = self.bind_query((**subquery).clone())?;
                Ok(Plan::Derived {
                    plan: Box::new(plan),
                    alias: alias.name.value.clone(),
                    columns: alias.columns.iter().map(|c| c.name.value.clone()).collect(),
                })
            }
            TableFactor::Table { name, alias, .. } => {
                let (db, table) = self.resolve_table_name(name)?;
                let alias = alias.as_ref().map(|a| a.name.value.clone());
                Ok(Plan::Scan { db, table, alias })
            }
            _ => Err(MySqlError::unsupported("table factor")),
        }
    }

    /// A window function call: `func(args) OVER (spec)` or `OVER name`.
    fn bind_window(
        &mut self,
        upper: &str,
        func: Function,
        over: sqlparser::ast::WindowType,
    ) -> Result<Expr, MySqlError> {
        if !self.window_ok {
            return Err(MySqlError::new(
                3593,
                "HY000",
                format!(
                    "You cannot use the window function '{}' in this context.'",
                    upper.to_lowercase()
                ),
            ));
        }
        let ast_spec = match over {
            sqlparser::ast::WindowType::WindowSpec(spec) => spec,
            sqlparser::ast::WindowType::NamedWindow(name) => self.named_window_spec(&name.value)?,
        };
        let args = match func.args {
            FunctionArguments::List(list) => {
                if matches!(
                    list.duplicate_treatment,
                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                ) {
                    return Err(MySqlError::unsupported(&format!(
                        "<window function>({upper} DISTINCT)"
                    )));
                }
                list.args
            }
            FunctionArguments::None => vec![],
            FunctionArguments::Subquery(_) => {
                return Err(MySqlError::unsupported("window function with subquery argument"));
            }
        };
        // Nothing inside a window may itself be a window function.
        self.window_ok = false;
        let res = self.bind_window_parts(upper, &args, ast_spec);
        self.window_ok = true;
        let (name, bound_args, spec) = res?;
        self.window_counter += 1;
        Ok(Expr::Window {
            id: self.window_counter,
            func: name,
            args: bound_args,
            spec: Box::new(spec),
        })
    }

    fn bind_window_parts(
        &mut self,
        upper: &str,
        args: &[FunctionArg],
        ast_spec: sqlparser::ast::WindowSpec,
    ) -> Result<(String, Vec<Expr>, plan::WindowSpec), MySqlError> {
        let star =
            args.len() == 1 && matches!(&args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard));
        let name = match upper {
            "COUNT" if star => "COUNT_STAR".to_string(),
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" | "NTILE"
            | "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" | "SUM" | "AVG"
            | "COUNT" | "MIN" | "MAX" => upper.to_string(),
            _ => {
                return Err(MySqlError::syntax_error(&format!(
                    "{upper}() is not a window function"
                )));
            }
        };
        let arity = match name.as_str() {
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" | "COUNT_STAR" => {
                0..=0
            }
            "LAG" | "LEAD" => 1..=3,
            "NTH_VALUE" => 2..=2,
            _ => 1..=1,
        };
        let bound_args = if star {
            vec![]
        } else {
            args.iter().map(|a| self.bind_function_arg(a)).collect::<Result<Vec<_>, _>>()?
        };
        if !arity.contains(&bound_args.len()) {
            return Err(MySqlError::new(
                1582,
                "42000",
                format!(
                    "Incorrect parameter count in the call to native function '{}'",
                    upper.to_lowercase()
                ),
            ));
        }
        let ast_spec = self.merge_base_window(ast_spec)?;
        let partition = ast_spec
            .partition_by
            .into_iter()
            .map(|e| self.bind_expr(e))
            .collect::<Result<_, _>>()?;
        let mut order = Vec::new();
        for o in ast_spec.order_by {
            let asc = !matches!(o.options.sort, Some(OrderBySort::Desc));
            order.push((self.bind_expr(o.expr)?, asc));
        }
        let frame = match ast_spec.window_frame {
            None => None,
            Some(f) => Some(self.bind_frame(f)?),
        };
        Ok((name, bound_args, plan::WindowSpec { partition, order, frame }))
    }

    /// `OVER (w ORDER BY ...)`: the named window `w` with this spec's own
    /// additions (MySQL lets the referring spec add ORDER BY and a frame,
    /// not PARTITION BY).
    fn merge_base_window(
        &self,
        spec: sqlparser::ast::WindowSpec,
    ) -> Result<sqlparser::ast::WindowSpec, MySqlError> {
        let Some(base_name) = spec.window_name.clone() else { return Ok(spec) };
        let base = self.merge_base_window(self.named_window_spec(&base_name.value)?)?;
        Ok(sqlparser::ast::WindowSpec {
            window_name: None,
            partition_by: base.partition_by,
            order_by: if spec.order_by.is_empty() { base.order_by } else { spec.order_by },
            window_frame: spec.window_frame.or(base.window_frame),
        })
    }

    fn named_window_spec(&self, name: &str) -> Result<sqlparser::ast::WindowSpec, MySqlError> {
        use sqlparser::ast::NamedWindowExpr;
        let def = self.named_windows.iter().find(|d| d.0.value.eq_ignore_ascii_case(name));
        match def.map(|d| &d.1) {
            Some(NamedWindowExpr::WindowSpec(s)) => Ok(s.clone()),
            Some(NamedWindowExpr::NamedWindow(other)) => self.named_window_spec(&other.value),
            None => {
                Err(MySqlError::new(3579, "HY000", format!("Window name '{name}' is not defined.")))
            }
        }
    }

    fn bind_frame(&mut self, f: sqlparser::ast::WindowFrame) -> Result<plan::Frame, MySqlError> {
        use sqlparser::ast::{WindowFrameBound as B, WindowFrameUnits as U};
        let rows = match f.units {
            U::Rows => true,
            U::Range => false,
            U::Groups => return Err(MySqlError::syntax_error("GROUPS frame")),
        };
        let offset = |e: &AstExpr| -> Result<u64, MySqlError> {
            match e {
                AstExpr::Value(sqlparser::ast::ValueWithSpan {
                    value: AstValue::Number(n, _),
                    ..
                }) if rows => {
                    n.parse::<u64>().map_err(|_| MySqlError::syntax_error("frame offset"))
                }
                _ => Err(MySqlError::unsupported("RANGE frame with an offset")),
            }
        };
        let bound = |b: &B| -> Result<plan::FrameBound, MySqlError> {
            Ok(match b {
                B::CurrentRow => plan::FrameBound::CurrentRow,
                B::Preceding(None) => plan::FrameBound::UnboundedPreceding,
                B::Following(None) => plan::FrameBound::UnboundedFollowing,
                B::Preceding(Some(e)) => plan::FrameBound::Preceding(offset(e)?),
                B::Following(Some(e)) => plan::FrameBound::Following(offset(e)?),
            })
        };
        let start = bound(&f.start_bound)?;
        let end = match &f.end_bound {
            Some(b) => bound(b)?,
            None => plan::FrameBound::CurrentRow,
        };
        Ok(plan::Frame { rows, start, end })
    }

    /// `expr op ALL|ANY|SOME (SELECT ...)`.
    fn bind_quantified(
        &mut self,
        left: AstExpr,
        op: BinaryOperator,
        right: AstExpr,
        all: bool,
    ) -> Result<Expr, MySqlError> {
        let op = match op {
            BinaryOperator::Eq => CmpOp::Eq,
            BinaryOperator::NotEq => CmpOp::Ne,
            BinaryOperator::Lt => CmpOp::Lt,
            BinaryOperator::LtEq => CmpOp::Le,
            BinaryOperator::Gt => CmpOp::Gt,
            BinaryOperator::GtEq => CmpOp::Ge,
            _ => return Err(MySqlError::unsupported("ALL/ANY operator")),
        };
        let mut right = right;
        while let AstExpr::Nested(inner) = right {
            right = *inner;
        }
        let AstExpr::Subquery(q) = right else {
            return Err(MySqlError::unsupported("ALL/ANY without a subquery"));
        };
        let expr = Box::new(self.bind_expr(left)?);
        Ok(Expr::Quantified { op, expr, plan: Box::new(self.bind_query(*q)?), all })
    }

    fn bind_join_constraint(&mut self, c: &JoinConstraint) -> Result<Expr, MySqlError> {
        match c {
            JoinConstraint::On(expr) => self.bind_expr(expr.clone()),
            _ => Err(MySqlError::unsupported("join constraint")),
        }
    }

    fn bind_expr(&mut self, expr: AstExpr) -> Result<Expr, MySqlError> {
        match expr {
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Number(s, _), ..
            }) => {
                // Found via testing before a public release: a non-integer
                // literal (`12.50`) used to bind as *text*, so a DECIMAL/
                // DOUBLE column filled from one stored strings -- SUM/AVG/
                // MAX of a money column came back NULL, ORDER BY sorted it
                // alphabetically, `price > 9.6` matched nothing. MySQL treats
                // `12.50` as an exact DECIMAL and `1.5e3` as a DOUBLE.
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Expr::Const(Value::Int(i)))
                } else if s.contains(['e', 'E']) {
                    s.parse::<f64>()
                        .map(|f| Expr::Const(Value::Float(f)))
                        .map_err(|_| MySqlError::syntax_error(&format!("bad number {s}")))
                } else {
                    crate::sql::numeric::Numeric::parse(&s)
                        .map(|n| Expr::Const(Value::Num(n)))
                        .map_err(|_| MySqlError::syntax_error(&format!("bad number {s}")))
                }
            }
            // MySQL's default (no ANSI_QUOTES) treats "..." as a string, not
            // an identifier -- common in PHP code.
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::SingleQuotedString(s) | AstValue::DoubleQuotedString(s),
                ..
            }) => Ok(Expr::Const(Value::Text(s))),
            // MySQL booleans are just 1/0.
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Boolean(b), ..
            }) => Ok(Expr::Const(Value::Int(b as i64))),
            AstExpr::Value(sqlparser::ast::ValueWithSpan { value: AstValue::Null, .. }) => {
                Ok(Expr::Const(Value::Null))
            }
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Placeholder(_),
                span,
            }) => {
                let at = (span.start.line, span.start.column);
                if let Ok(rank) = self.placeholder_at.binary_search(&at) {
                    return Ok(Expr::Param(rank));
                }
                let idx = self.param_counter;
                self.param_counter += 1;
                Ok(Expr::Param(idx))
            }
            AstExpr::Function(func) => self.bind_function(func),
            // `CURRENT_TIMESTAMP`/`CURRENT_DATE` written without parentheses
            // (the usual form) arrive as a bare identifier, not a function
            // call -- previously bound as a column name, which silently
            // resolved to NULL.
            AstExpr::Identifier(ident)
                if ident.quote_style.is_none()
                    && matches!(
                        ident.value.to_ascii_uppercase().as_str(),
                        "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "LOCALTIMESTAMP" | "LOCALTIME"
                    ) =>
            {
                Ok(Expr::Call { name: ident.value.to_ascii_uppercase(), args: vec![] })
            }
            AstExpr::Identifier(ident) => {
                if ident.value.starts_with("@@") {
                    Ok(Expr::SysVar(ident.value[2..].to_string()))
                } else {
                    Ok(Expr::ColName(ident.value.clone()))
                }
            }
            // A qualified column reference (`table.col`, or even
            // `db.table.col`). Found via testing before a public release:
            // this used to drop every qualifier and keep only the final
            // part, on the claim that "joins aren't bound through this
            // path" -- false, a join's own ON condition (and any SELECT
            // list/WHERE referencing both sides) binds through exactly
            // this arm. Dropping the qualifier meant `cust.id` and
            // `orders.id` (any two joined tables sharing a column name --
            // extremely common, e.g. every table having its own `id`)
            // silently resolved to the *same* physical column, whichever
            // table's happened to come first, regardless of which one was
            // actually named: joining on `cust.id = orders.customer_id`
            // and then selecting `orders.id` would silently return
            // `cust.id`'s value instead. Now the full dotted path is kept,
            // and `Executor::eval_expr`'s `ColName` lookup (see its own
            // comment) tries an exact qualified match first before
            // falling back to a bare-name match -- so a qualified
            // reference against a join's merged, qualified column list
            // resolves to the real table it names, and a plain unqualified
            // reference still works exactly as before for the ordinary
            // single-table case.
            AstExpr::CompoundIdentifier(idents) => {
                if idents.is_empty() {
                    return Err(MySqlError::unsupported("empty compound identifier"));
                }
                let dotted = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                Ok(Expr::ColName(dotted))
            }
            AstExpr::BinaryOp { left, op, right } => {
                let l = self.bind_expr(*left)?;
                let r = self.bind_expr(*right)?;
                // `d + INTERVAL 1 DAY` / `d - INTERVAL 1 DAY`.
                if let Expr::Call { name, args } = &r
                    && name == "INTERVAL"
                    && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus)
                {
                    let f =
                        if matches!(op, BinaryOperator::Plus) { "DATE_ADD" } else { "DATE_SUB" };
                    let mut a = vec![l];
                    a.extend(args.iter().cloned());
                    return Ok(Expr::Call { name: f.into(), args: a });
                }
                let call = |n: &str, args: Vec<Expr>| Ok(Expr::Call { name: n.into(), args });
                match op {
                    // `col->'$.a'` is JSON_EXTRACT, `col->>'$.a'` also unquotes.
                    BinaryOperator::Arrow => return call("JSON_EXTRACT", vec![l, r]),
                    BinaryOperator::LongArrow => {
                        return call(
                            "JSON_UNQUOTE",
                            vec![Expr::Call { name: "JSON_EXTRACT".into(), args: vec![l, r] }],
                        );
                    }
                    BinaryOperator::MyIntegerDivide => return call("DIV", vec![l, r]),
                    _ => {}
                }
                match op {
                    BinaryOperator::Eq => {
                        Ok(Expr::Compare { op: CmpOp::Eq, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::NotEq => {
                        Ok(Expr::Compare { op: CmpOp::Ne, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Lt => {
                        Ok(Expr::Compare { op: CmpOp::Lt, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::LtEq => {
                        Ok(Expr::Compare { op: CmpOp::Le, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Gt => {
                        Ok(Expr::Compare { op: CmpOp::Gt, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::GtEq => {
                        Ok(Expr::Compare { op: CmpOp::Ge, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::And => Ok(Expr::And(vec![l, r])),
                    BinaryOperator::Or => Ok(Expr::Or(vec![l, r])),
                    BinaryOperator::Plus => {
                        Ok(Expr::Arith { op: ArithOp::Add, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Minus => {
                        Ok(Expr::Arith { op: ArithOp::Sub, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Multiply => {
                        Ok(Expr::Arith { op: ArithOp::Mul, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Divide => {
                        Ok(Expr::Arith { op: ArithOp::Div, left: Box::new(l), right: Box::new(r) })
                    }
                    BinaryOperator::Modulo => {
                        Ok(Expr::Arith { op: ArithOp::Mod, left: Box::new(l), right: Box::new(r) })
                    }
                    _ => Err(MySqlError::unsupported("binary operator")),
                }
            }
            AstExpr::InList { expr, list, negated } => {
                let bound_expr = Box::new(self.bind_expr(*expr)?);
                let bound_list =
                    list.into_iter().map(|e| self.bind_expr(e)).collect::<Result<_, _>>()?;
                Ok(Expr::InList { expr: bound_expr, list: bound_list, negated })
            }
            AstExpr::Nested(inner) => self.bind_expr(*inner),
            AstExpr::Subquery(q) => Ok(Expr::Subquery(Box::new(self.bind_query(*q)?))),
            AstExpr::InSubquery { expr, subquery, negated } => {
                let expr = Box::new(self.bind_expr(*expr)?);
                let plan = Box::new(self.bind_query(*subquery)?);
                Ok(Expr::InSubquery { expr, plan, negated })
            }
            AstExpr::AnyOp { left, compare_op, right, .. } => {
                self.bind_quantified(*left, compare_op, *right, false)
            }
            AstExpr::AllOp { left, compare_op, right } => {
                self.bind_quantified(*left, compare_op, *right, true)
            }
            AstExpr::Exists { subquery, negated } => {
                Ok(Expr::Exists { plan: Box::new(self.bind_query(*subquery)?), negated })
            }
            AstExpr::Interval(iv) => {
                let unit = iv
                    .leading_field
                    .as_ref()
                    .map(|f| f.to_string().to_uppercase())
                    .ok_or_else(|| MySqlError::unsupported("INTERVAL without a unit"))?;
                let v = self.bind_expr(*iv.value)?;
                Ok(Expr::Call {
                    name: "INTERVAL".into(),
                    args: vec![v, Expr::Const(Value::Text(unit))],
                })
            }
            AstExpr::Trim { trim_where, trim_what, expr, .. } => {
                let side = match trim_where {
                    Some(sqlparser::ast::TrimWhereField::Leading) => "LTRIM",
                    Some(sqlparser::ast::TrimWhereField::Trailing) => "RTRIM",
                    _ => "TRIM",
                };
                let mut args = vec![self.bind_expr(*expr)?];
                if let Some(w) = trim_what {
                    args.push(self.bind_expr(*w)?);
                }
                Ok(Expr::Call { name: side.into(), args })
            }
            AstExpr::Cast { expr, data_type, .. } => {
                // The target type travels as its SQL spelling (`SIGNED`,
                // `CHAR(10)`, `DECIMAL(10,2)`); see `cast_value`.
                let ty = data_type.to_string().to_uppercase();
                let e = self.bind_expr(*expr)?;
                Ok(Expr::Call { name: "CAST".into(), args: vec![e, Expr::Const(Value::Text(ty))] })
            }
            AstExpr::Ceil { expr, .. } => {
                Ok(Expr::Call { name: "CEIL".into(), args: vec![self.bind_expr(*expr)?] })
            }
            AstExpr::Floor { expr, .. } => {
                Ok(Expr::Call { name: "FLOOR".into(), args: vec![self.bind_expr(*expr)?] })
            }
            AstExpr::Position { expr, r#in } => Ok(Expr::Call {
                name: "LOCATE".into(),
                args: vec![self.bind_expr(*expr)?, self.bind_expr(*r#in)?],
            }),
            AstExpr::Extract { field, expr, .. } => Ok(Expr::Call {
                name: "EXTRACT".into(),
                args: vec![
                    Expr::Const(Value::Text(field.to_string().to_uppercase())),
                    self.bind_expr(*expr)?,
                ],
            }),
            AstExpr::UnaryOp { op: UnaryOperator::Not, expr } => {
                Ok(Expr::Not(Box::new(self.bind_expr(*expr)?)))
            }
            AstExpr::UnaryOp { op: UnaryOperator::Minus, expr } => {
                // No literal negative-number AST node in sqlparser for this
                // dialect's integer literals -- `-5` arrives as a unary
                // minus over `5`. Expressed as `0 - expr` rather than a new
                // `Expr` variant, since every numeric `Expr` already
                // supports `Arith`.
                let e = self.bind_expr(*expr)?;
                Ok(Expr::Arith {
                    op: ArithOp::Sub,
                    left: Box::new(Expr::Const(Value::Int(0))),
                    right: Box::new(e),
                })
            }
            AstExpr::UnaryOp { op: UnaryOperator::Plus, expr } => self.bind_expr(*expr),
            AstExpr::IsNull(e) => Ok(Expr::IsNull(Box::new(self.bind_expr(*e)?), false)),
            AstExpr::IsNotNull(e) => Ok(Expr::IsNull(Box::new(self.bind_expr(*e)?), true)),
            AstExpr::Between { expr, negated, low, high } => {
                let x = self.bind_expr(*expr)?;
                let lo = self.bind_expr(*low)?;
                let hi = self.bind_expr(*high)?;
                let ge =
                    Expr::Compare { op: CmpOp::Ge, left: Box::new(x.clone()), right: Box::new(lo) };
                let le = Expr::Compare { op: CmpOp::Le, left: Box::new(x), right: Box::new(hi) };
                let both = Expr::And(vec![ge, le]);
                Ok(if negated { Expr::Not(Box::new(both)) } else { both })
            }
            AstExpr::Like { negated, expr, pattern, escape_char, any } => {
                if any {
                    return Err(MySqlError::unsupported("LIKE ANY"));
                }
                let e = self.bind_expr(*expr)?;
                let p = self.bind_expr(*pattern)?;
                let esc = match escape_char {
                    Some(c) => self.bind_expr(*c)?,
                    // MySQL's own default when no `ESCAPE` clause is given.
                    None => Expr::Const(Value::Text("\\".to_string())),
                };
                Ok(Expr::Like {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                    escape: Box::new(esc),
                    negated,
                })
            }
            AstExpr::Substring { expr, substring_from, substring_for, .. } => {
                // sqlparser parses any `SUBSTRING(...)` call -- both the
                // comma form this engine's executor expects and the
                // `FROM ... FOR ...` SQL-standard form -- into this
                // dedicated AST node, never a plain `AstExpr::Function`.
                let mut args = vec![self.bind_expr(*expr)?];
                if let Some(f) = substring_from {
                    args.push(self.bind_expr(*f)?);
                }
                if let Some(l) = substring_for {
                    args.push(self.bind_expr(*l)?);
                }
                Ok(Expr::Call { name: "SUBSTRING".to_string(), args })
            }
            AstExpr::Case { operand, conditions, else_result, .. } => {
                let bound_operand = match operand {
                    Some(o) => Some(self.bind_expr(*o)?),
                    None => None,
                };
                let mut bound_conditions = Vec::with_capacity(conditions.len());
                for w in conditions {
                    let cond = self.bind_expr(w.condition)?;
                    let result = self.bind_expr(w.result)?;
                    // A simple `CASE x WHEN v THEN r` compares `x = v`; a
                    // searched `CASE WHEN cond THEN r` uses `cond` as-is.
                    let cond = match &bound_operand {
                        Some(op) => Expr::Compare {
                            op: CmpOp::Eq,
                            left: Box::new(op.clone()),
                            right: Box::new(cond),
                        },
                        None => cond,
                    };
                    bound_conditions.push((cond, result));
                }
                let bound_else = match else_result {
                    Some(e) => Some(Box::new(self.bind_expr(*e)?)),
                    None => None,
                };
                Ok(Expr::Case { conditions: bound_conditions, else_result: bound_else })
            }
            _ => Err(MySqlError::unsupported("expr")),
        }
    }

    /// Binds `COUNT`/`COUNT(*)`/`SUM`/`AVG`/`MIN`/`MAX` calls to
    /// `Expr::Agg`. Any other function name is unsupported — this engine
    /// has no scalar function library yet.
    fn bind_function(&mut self, func: Function) -> Result<Expr, MySqlError> {
        let name = func
            .name
            .0
            .iter()
            .filter_map(|p| match p {
                sqlparser::ast::ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(".");
        let upper = name.to_uppercase();
        if let Some(over) = func.over.clone() {
            return self.bind_window(&upper, func, over);
        }

        let mut agg_distinct = false;
        let mut clauses = Vec::new();
        let args = match func.args {
            FunctionArguments::List(list) => {
                agg_distinct = matches!(
                    list.duplicate_treatment,
                    Some(sqlparser::ast::DuplicateTreatment::Distinct)
                );
                clauses = list.clauses;
                list.args
            }
            FunctionArguments::None => vec![],
            FunctionArguments::Subquery(_) => {
                return Err(MySqlError::unsupported("function with subquery argument"));
            }
        };

        match upper.as_str() {
            "COUNT" => {
                if args.len() == 1
                    && matches!(&args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                {
                    Ok(Expr::Agg { func: AggFunc::CountStar, arg: None, distinct: false })
                } else if args.len() == 1 {
                    let arg = self.bind_function_arg(&args[0])?;
                    Ok(Expr::Agg {
                        func: AggFunc::Count,
                        arg: Some(Box::new(arg)),
                        distinct: agg_distinct,
                    })
                } else {
                    Err(MySqlError::unsupported("COUNT argument list"))
                }
            }
            "SUM" | "AVG" | "MIN" | "MAX" => {
                if args.len() != 1 {
                    return Err(MySqlError::unsupported(&format!("{upper} argument list")));
                }
                let arg = self.bind_function_arg(&args[0])?;
                let agg_func = match upper.as_str() {
                    "SUM" => AggFunc::Sum,
                    "AVG" => AggFunc::Avg,
                    "MIN" => AggFunc::Min,
                    "MAX" => AggFunc::Max,
                    _ => unreachable!(),
                };
                Ok(Expr::Agg { func: agg_func, arg: Some(Box::new(arg)), distinct: agg_distinct })
            }
            "FOUND_ROWS" => {
                if !args.is_empty() {
                    return Err(MySqlError::unsupported("FOUND_ROWS argument list"));
                }
                Ok(Expr::FoundRows)
            }
            // A small, fixed scalar-function library (no general catalog)
            // -- see `Executor::eval_call` for what each one actually
            // computes. Argument-count validation happens there too, not
            // here, so it stays next to the logic it's validating.
            // `VALUES(col)` inside ON DUPLICATE KEY UPDATE: the value this
            // row would have been inserted with (see `InsertMode::Upsert`).
            "VALUES" => {
                if args.len() != 1 {
                    return Err(MySqlError::unsupported("VALUES argument list"));
                }
                let arg = self.bind_function_arg(&args[0])?;
                Ok(Expr::Call { name: "VALUES".to_string(), args: vec![arg] })
            }
            "LAST_INSERT_ID" if args.is_empty() => Ok(Expr::Call { name: upper, args: vec![] }),
            // `GROUP_CONCAT([DISTINCT] a, b ... [ORDER BY x [DESC]] [SEPARATOR s])`.
            // The aggregate's argument is a `GROUP_CONCAT` call carrying the
            // concatenated value, the separator, then (key, asc) order pairs;
            // `Executor::fold_aggs` evaluates it per row.
            "GROUP_CONCAT" => {
                let mut parts = Vec::new();
                for a in &args {
                    parts.push(self.bind_function_arg(a)?);
                }
                if parts.is_empty() {
                    return Err(MySqlError::unsupported("GROUP_CONCAT argument list"));
                }
                let value = if parts.len() == 1 {
                    parts.remove(0)
                } else {
                    Expr::Call { name: "CONCAT".into(), args: parts }
                };
                let mut sep = ",".to_string();
                let mut order = Vec::new();
                for clause in &clauses {
                    match clause {
                        sqlparser::ast::FunctionArgumentClause::Separator(v) => {
                            sep = match &v.value {
                                AstValue::SingleQuotedString(s)
                                | AstValue::DoubleQuotedString(s) => s.clone(),
                                other => other.to_string(),
                            };
                        }
                        sqlparser::ast::FunctionArgumentClause::OrderBy(items) => {
                            for item in items {
                                let asc = !matches!(item.options.sort, Some(OrderBySort::Desc));
                                order.push(self.bind_expr(item.expr.clone())?);
                                order.push(Expr::Const(Value::Int(asc as i64)));
                            }
                        }
                        _ => return Err(MySqlError::unsupported("GROUP_CONCAT clause")),
                    }
                }
                let mut call_args = vec![value, Expr::Const(Value::Text(sep))];
                call_args.extend(order);
                Ok(Expr::Agg {
                    func: AggFunc::GroupConcat,
                    arg: Some(Box::new(Expr::Call {
                        name: "GROUP_CONCAT".into(),
                        args: call_args,
                    })),
                    distinct: agg_distinct,
                })
            }
            // `DATE_ADD(d, INTERVAL n unit)` -> DATE_ADD(d, n, unit).
            "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE" => {
                if args.len() != 2 {
                    return Err(MySqlError::unsupported(&format!("{upper} argument list")));
                }
                let d = self.bind_function_arg(&args[0])?;
                let f =
                    if upper == "DATE_ADD" || upper == "ADDDATE" { "DATE_ADD" } else { "DATE_SUB" };
                match self.bind_function_arg(&args[1])? {
                    Expr::Call { name, args: iv } if name == "INTERVAL" => {
                        let mut a = vec![d];
                        a.extend(iv);
                        Ok(Expr::Call { name: f.into(), args: a })
                    }
                    // `ADDDATE(d, 3)`: days.
                    n => Ok(Expr::Call {
                        name: f.into(),
                        args: vec![d, n, Expr::Const(Value::Text("DAY".into()))],
                    }),
                }
            }
            // `TIMESTAMPDIFF(unit, a, b)`: the unit is a bare keyword.
            "TIMESTAMPDIFF" | "TIMESTAMPADD" => {
                if args.len() != 3 {
                    return Err(MySqlError::unsupported(&format!("{upper} argument list")));
                }
                let unit = match &args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(AstExpr::Identifier(i))) => {
                        i.value.to_uppercase()
                    }
                    _ => return Err(MySqlError::unsupported(&format!("{upper} unit"))),
                };
                let a = self.bind_function_arg(&args[1])?;
                let b = self.bind_function_arg(&args[2])?;
                Ok(Expr::Call { name: upper, args: vec![Expr::Const(Value::Text(unit)), a, b] })
            }
            "CONCAT" | "UPPER" | "LOWER" | "LENGTH" | "SUBSTRING" | "SUBSTR" | "COALESCE"
            | "IFNULL" | "DATABASE" | "SCHEMA" | "USER" | "CURRENT_USER" | "SESSION_USER"
            | "SYSTEM_USER" | "CONNECTION_ID" | "VERSION" | "NOW" | "CURRENT_TIMESTAMP"
            | "LOCALTIMESTAMP" | "LOCALTIME" | "SYSDATE" | "CURDATE" | "CURRENT_DATE" | "IF"
            | "NULLIF" | "GREATEST" | "LEAST" | "ROUND" | "TRUNCATE" | "ABS" | "CEIL"
            | "CEILING" | "FLOOR" | "MOD" | "POW" | "POWER" | "SQRT" | "SIGN" | "CHAR_LENGTH"
            | "CHARACTER_LENGTH" | "CONCAT_WS" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
            | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "REPEAT" | "REVERSE" | "LOCATE" | "INSTR"
            | "UCASE" | "LCASE" | "MID" | "DATE" | "TIME" | "YEAR" | "MONTH" | "DAY"
            | "DAYOFMONTH" | "HOUR" | "MINUTE" | "SECOND" | "DAYOFWEEK" | "DAYOFYEAR"
            | "WEEKDAY" | "DATE_FORMAT" | "DATEDIFF" | "UNIX_TIMESTAMP" | "FROM_UNIXTIME"
            | "UTC_TIMESTAMP" | "UTC_DATE" | "LAST_DAY" | "JSON_EXTRACT" | "JSON_UNQUOTE"
            | "JSON_OBJECT" | "JSON_ARRAY" | "JSON_VALID" | "JSON_TYPE" | "JSON_LENGTH" | "HEX"
            | "FIELD" | "ELT" | "STRCMP" | "CONVERT_TZ" | "JSON_CONTAINS"
            | "JSON_CONTAINS_PATH" | "JSON_KEYS" => {
                let bound_args =
                    args.iter().map(|a| self.bind_function_arg(a)).collect::<Result<_, _>>()?;
                Ok(Expr::Call { name: upper, args: bound_args })
            }
            _ => Err(MySqlError::unsupported(&format!("function {name}"))),
        }
    }

    fn bind_function_arg(&mut self, arg: &FunctionArg) -> Result<Expr, MySqlError> {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => self.bind_expr(e.clone()),
            _ => Err(MySqlError::unsupported("function argument form")),
        }
    }

    fn bind_foreign_key(
        &self,
        fk: &sqlparser::ast::ForeignKeyConstraint,
    ) -> Result<crate::mysql::catalog::ForeignKey, MySqlError> {
        use crate::mysql::catalog::FkAction;
        use sqlparser::ast::ReferentialAction as R;
        let (ref_db, ref_table) = self.resolve_table_name(&fk.foreign_table)?;
        let action = |a: &Option<R>| -> Result<FkAction, MySqlError> {
            Ok(match a {
                None | Some(R::NoAction) => FkAction::NoAction,
                Some(R::Restrict) => FkAction::Restrict,
                Some(R::Cascade) => FkAction::Cascade,
                Some(R::SetNull) => FkAction::SetNull,
                Some(R::SetDefault) => {
                    return Err(MySqlError::unsupported("SET DEFAULT foreign key action"));
                }
            })
        };
        Ok(crate::mysql::catalog::ForeignKey {
            name: fk.name.as_ref().map(|n| n.value.clone()).unwrap_or_default(),
            columns: fk.columns.iter().map(|c| c.value.clone()).collect(),
            ref_db,
            ref_table,
            ref_columns: fk.referred_columns.iter().map(|c| c.value.clone()).collect(),
            on_delete: action(&fk.on_delete)?,
            on_update: action(&fk.on_update)?,
            // MySQL names the index after the CONSTRAINT symbol first.
            index: fk
                .name
                .as_ref()
                .or(fk.index_name.as_ref())
                .map(|n| n.value.clone())
                .unwrap_or_default(),
        })
    }

    fn resolve_table_name(&self, name: &ObjectName) -> Result<(String, String), MySqlError> {
        if name.0.len() == 1 {
            let db = self
                .current_db
                .clone()
                .ok_or_else(|| MySqlError::new(1046, "3D000", "No database selected"))?;
            Ok((
                db,
                match &name.0[0] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err(MySqlError::syntax_error("bad identifier")),
                },
            ))
        } else if name.0.len() == 2 {
            Ok((
                match &name.0[0] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err(MySqlError::syntax_error("bad identifier")),
                },
                match &name.0[1] {
                    sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.clone(),
                    _ => return Err(MySqlError::syntax_error("bad identifier")),
                },
            ))
        } else {
            Err(MySqlError::syntax_error("invalid table name"))
        }
    }
}

/// Extracts a `LIMIT`/`OFFSET` value: real MySQL only accepts a plain
/// non-negative integer literal there (no expressions, no placeholders),
/// so this rejects anything else rather than trying to evaluate it.
impl Binder {
    /// A LIMIT/OFFSET: a non-negative integer literal or a `?`. Found via
    /// testing before a public release: `LIMIT ?` (how every Node/Go/JDBC
    /// app paginates with a prepared statement) was rejected.
    fn bind_count(&mut self, expr: AstExpr) -> Result<Expr, MySqlError> {
        match &expr {
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Number(s, _), ..
            }) if s.parse::<u64>().is_ok() => {
                Ok(Expr::Const(Value::Int(s.parse::<i64>().unwrap_or(i64::MAX))))
            }
            AstExpr::Value(sqlparser::ast::ValueWithSpan {
                value: AstValue::Placeholder(_),
                ..
            }) => self.bind_expr(expr),
            _ => Err(MySqlError::syntax_error("LIMIT/OFFSET must be an integer or ?")),
        }
    }
}

/// `CURRENT_TIMESTAMP`, `CURRENT_TIMESTAMP()`, `NOW()`, `LOCALTIMESTAMP`,
/// `CURRENT_DATE` -- the column default / `ON UPDATE` values meaning "the
/// time of the write", which can't be stored as a constant.
fn is_current_time(e: &AstExpr) -> bool {
    let name = match e {
        AstExpr::Identifier(i) => i.value.clone(),
        AstExpr::Function(f) => f.name.to_string(),
        _ => return false,
    };
    matches!(
        name.to_ascii_uppercase().as_str(),
        "CURRENT_TIMESTAMP" | "NOW" | "LOCALTIMESTAMP" | "LOCALTIME" | "CURRENT_DATE" | "CURDATE"
    )
}

/// Replaces a bare `ColName` that names a SELECT-list alias with the
/// aliased expression itself (`HAVING cnt > 2` where `COUNT(*) AS cnt`).
fn substitute_aliases(e: Expr, aliases: &[(String, Expr)]) -> Expr {
    let sub = |x: Expr| substitute_aliases(x, aliases);
    let sub_box = |x: Box<Expr>| Box::new(substitute_aliases(*x, aliases));
    match e {
        Expr::ColName(ref n) if !n.contains('.') => aliases
            .iter()
            .find(|(a, _)| a.eq_ignore_ascii_case(n))
            .map(|(_, x)| x.clone())
            .unwrap_or(e),
        Expr::And(v) => Expr::And(v.into_iter().map(sub).collect()),
        Expr::Or(v) => Expr::Or(v.into_iter().map(sub).collect()),
        Expr::Compare { op, left, right } => {
            Expr::Compare { op, left: sub_box(left), right: sub_box(right) }
        }
        Expr::Arith { op, left, right } => {
            Expr::Arith { op, left: sub_box(left), right: sub_box(right) }
        }
        Expr::Call { name, args } => Expr::Call { name, args: args.into_iter().map(sub).collect() },
        Expr::InList { expr, list, negated } => {
            Expr::InList { expr: sub_box(expr), list: list.into_iter().map(sub).collect(), negated }
        }
        Expr::Not(x) => Expr::Not(sub_box(x)),
        Expr::IsNull(x, neg) => Expr::IsNull(sub_box(x), neg),
        Expr::Like { expr, pattern, escape, negated } => Expr::Like {
            expr: sub_box(expr),
            pattern: sub_box(pattern),
            escape: sub_box(escape),
            negated,
        },
        Expr::Case { conditions, else_result } => Expr::Case {
            conditions: conditions.into_iter().map(|(c, r)| (sub(c), sub(r))).collect(),
            else_result: else_result.map(sub_box),
        },
        other => other,
    }
}

/// The column an `UPDATE`/`INSERT ... SET`/`ON DUPLICATE KEY UPDATE`
/// assignment targets: `col`, or a table-qualified `t.col`.
fn assignment_column(target: &sqlparser::ast::AssignmentTarget) -> Result<String, MySqlError> {
    match target {
        sqlparser::ast::AssignmentTarget::ColumnName(name) if name.0.len() <= 2 => {
            match name.0.last() {
                Some(sqlparser::ast::ObjectNamePart::Identifier(id)) => Ok(id.value.clone()),
                _ => Err(MySqlError::unsupported("assignment column name format")),
            }
        }
        _ => Err(MySqlError::unsupported("assignment target")),
    }
}

/// `new.col` (an `INSERT ... AS new` row alias) -> `VALUES(col)`.
fn row_alias_to_values(e: Expr, alias: &str) -> Expr {
    crate::mysql::plan::map_colnames(e, &|n| match n.split_once('.') {
        Some((a, col)) if a.eq_ignore_ascii_case(alias) => {
            Expr::Call { name: "VALUES".to_string(), args: vec![Expr::ColName(col.to_string())] }
        }
        _ => Expr::ColName(n),
    })
}

/// `UPDATE users u ... WHERE u.id = 1`: drops the single target table's
/// alias qualifier, leaving the plain column name.
fn strip_alias(e: Expr, alias: Option<&str>) -> Expr {
    let Some(alias) = alias else { return e };
    crate::mysql::plan::map_colnames(e, &|n| match n.split_once('.') {
        Some((a, col)) if a.eq_ignore_ascii_case(alias) => Expr::ColName(col.to_string()),
        _ => Expr::ColName(n),
    })
}

/// `ORDER BY` keys as (expression, ascending).
type OrderKeys = Vec<(Expr, bool)>;

/// sqlparser's MODIFY/CHANGE carry bare `ColumnOption`s; `ColumnDef` wants
/// them wrapped with (no) constraint names.
fn options_with_names(
    options: Vec<sqlparser::ast::ColumnOption>,
) -> Vec<sqlparser::ast::ColumnOptionDef> {
    options
        .into_iter()
        .map(|option| sqlparser::ast::ColumnOptionDef { name: None, option })
        .collect()
}
