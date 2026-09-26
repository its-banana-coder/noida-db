//! CREATE / ALTER / DROP and other schema changes.

use sqlparser::ast as a;

use super::binder::{Binder, SessionInfo, name_parts, unsupported};
use super::catalog::*;
use super::error::{PgError, PgResult, code};
use super::exec::Ctx;
use super::plan::Expr;
use super::types::{self, Base, Type, Value};

pub struct Ddl<'a, 'b> {
    pub ctx: &'a mut Ctx<'b>,
    pub info: SessionInfo,
}

fn ident(id: &a::Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_lowercase(),
    }
}

impl Ddl<'_, '_> {
    /// The schema a new object goes into, and its name.
    fn target(&self, parts: &[String]) -> PgResult<(u32, String)> {
        let name = parts.last().cloned().unwrap_or_default();
        let schema = if parts.len() > 1 {
            let s = &parts[parts.len() - 2];
            self.ctx.db.schema_by_name(s).ok_or_else(|| {
                PgError::new(code::INVALID_SCHEMA_NAME, format!("schema \"{s}\" does not exist"))
            })?
        } else {
            let mut found = None;
            for s in &self.info.search_path {
                if let Some(oid) = self.ctx.db.schema_by_name(s) {
                    found = Some(oid);
                    break;
                }
            }
            found.ok_or_else(|| {
                PgError::new(code::INVALID_SCHEMA_NAME, "no schema has been selected to create in")
            })?
        };
        Ok((schema, types::truncate_name(&name)))
    }

    fn binder(&self) -> (DbState, SessionInfo) {
        (
            self.ctx.db.clone(),
            SessionInfo {
                user: self.info.user.clone(),
                database: self.info.database.clone(),
                search_path: self.info.search_path.clone(),
                fmt: self.info.fmt.clone(),
                now: self.info.now,
            },
        )
    }

    pub fn create_table(&mut self, ct: &a::CreateTable) -> PgResult<String> {
        let parts = name_parts(&ct.name);
        let (schema, name) = self.target(&parts)?;
        if ct.temporary {
            // Temporary tables live in the session's own schema; noida keeps
            // them in the normal one, which is fine for a single connection.
        }
        if self.ctx.db.relation_exists(schema, &name) {
            if ct.if_not_exists {
                return Ok(format!("relation \"{name}\" already exists, skipping"));
            }
            return Err(PgError::new(
                code::DUPLICATE_TABLE,
                format!("relation \"{name}\" already exists"),
            ));
        }
        let oid = self.ctx.db.alloc_oid();
        let type_oid = self.ctx.db.alloc_oid();
        let mut table = Table {
            oid,
            name: name.clone(),
            schema,
            kind: RelKind::Table,
            columns: vec![],
            rows: vec![],
            constraints: vec![],
            indexes: vec![],
            view_sql: None,
            comment: None,
            type_oid,
            temp: ct.temporary,
            owner_session: None,
        };
        // CREATE TABLE AS SELECT
        if let Some(q) = &ct.query {
            let (db, info) = self.binder();
            let mut b = Binder::new(&db, &info, &[]);
            let (plan, cols) = b.bind_query(q)?;
            for c in &cols {
                table.columns.push(Column { typmod: c.typmod, ..Column::new(&c.name, c.ty) });
            }
            let rows = super::exec::run_query(&plan, self.ctx)?;
            table.rows = rows;
            self.ctx.db.tables.insert(oid, std::sync::Arc::new(table));
            return Ok(format!("SELECT {}", self.ctx.db.table(oid).unwrap().rows.len()));
        }
        let mut pending: Vec<(String, Vec<String>, PendingConstraint)> = vec![];
        let mut serial_cols: Vec<usize> = vec![];
        for col in &ct.columns {
            let cname = ident(&col.name);
            let (ty, typmod, serial) = self.column_type(&col.data_type)?;
            let mut c = Column { typmod, ..Column::new(&cname, ty) };
            for opt in &col.options {
                match &opt.option {
                    a::ColumnOption::NotNull => c.not_null = true,
                    a::ColumnOption::Null => {}
                    a::ColumnOption::Default(e) => c.default = Some(e.to_string()),
                    a::ColumnOption::PrimaryKey(pk) => {
                        c.not_null = true;
                        let n =
                            pk.name.as_ref().or(opt.name.as_ref()).map(ident).unwrap_or_default();
                        pending.push((n, vec![cname.clone()], PendingConstraint::PrimaryKey));
                    }
                    a::ColumnOption::Unique(u) => {
                        let n =
                            u.name.as_ref().or(opt.name.as_ref()).map(ident).unwrap_or_default();
                        let nd = !matches!(u.nulls_distinct, a::NullsDistinctOption::NotDistinct);
                        pending.push((n, vec![cname.clone()], PendingConstraint::UniqueNulls(nd)));
                    }
                    a::ColumnOption::ForeignKey(fk) => {
                        pending.push((
                            fk.name.as_ref().or(opt.name.as_ref()).map(ident).unwrap_or_default(),
                            vec![cname.clone()],
                            PendingConstraint::ForeignKey {
                                table: name_parts(&fk.foreign_table),
                                cols: fk.referred_columns.iter().map(ident).collect(),
                                on_delete: fk_action(&fk.on_delete),
                                on_update: fk_action(&fk.on_update),
                            },
                        ));
                    }
                    a::ColumnOption::Check(chk) => {
                        pending.push((
                            chk.name.as_ref().or(opt.name.as_ref()).map(ident).unwrap_or_default(),
                            vec![cname.clone()],
                            PendingConstraint::Check(chk.expr.to_string()),
                        ));
                    }
                    a::ColumnOption::Generated { generated_as, generation_expr, .. } => {
                        match generation_expr {
                            Some(e) => c.generated = Some(e.to_string()),
                            None => {
                                let always = matches!(generated_as, a::GeneratedAs::Always);
                                c.identity = Some((always, 0));
                                c.not_null = true;
                            }
                        }
                    }
                    a::ColumnOption::Comment(cm) => c.comment = Some(cm.clone()),
                    a::ColumnOption::Collation(_) => {}
                    other => return Err(unsupported(&format!("column option {other}"))),
                }
            }
            if serial {
                // Marked here, turned into a nextval() default below.
                c.identity = Some((false, 0));
                c.not_null = true;
                serial_cols.push(table.columns.len());
            }
            table.columns.push(c);
        }
        for cons in &ct.constraints {
            pending.push(self.table_constraint(cons)?);
        }
        // Columns needing a sequence (serial / identity).
        let cols_needing_seq: Vec<usize> = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.identity.is_some())
            .map(|(i, _)| i)
            .collect();
        self.ctx.db.tables.insert(oid, std::sync::Arc::new(table));
        for i in cols_needing_seq {
            let (cname, ty) = {
                let t = self.ctx.db.table(oid).unwrap();
                (t.columns[i].name.clone(), t.columns[i].ty)
            };
            let seq_name =
                self.ctx.db.unique_rel_name(schema, &make_object_name(&name, Some(&cname), "seq"));
            let seq_oid = self.create_sequence_object(schema, &seq_name, ty, Some((oid, i)))?;
            let t = self.ctx.db.table_mut(oid).unwrap();
            if serial_cols.contains(&i) {
                t.columns[i].identity = None;
                t.columns[i].default = Some(format!("nextval('{seq_name}'::regclass)"));
            } else {
                t.columns[i].identity = Some((t.columns[i].identity.unwrap().0, seq_oid));
            }
        }
        for (cname, cols, kind) in pending {
            self.add_constraint(oid, &cname, &cols, kind)?;
        }
        // Validate defaults and checks by binding them once.
        self.validate_table(oid)?;
        Ok("CREATE TABLE".into())
    }

    fn validate_table(&mut self, oid: u32) -> PgResult<()> {
        let (db, info) = self.binder();
        let t = db.table(oid).unwrap();
        let mut b = Binder::new(&db, &info, &[]);
        for c in &t.columns {
            if let Some(d) = &c.default {
                let te = b.bind_sql_expr(d)?;
                b.coerce(te, c.ty, c.typmod, super::casts::CastCtx::Assignment, &c.name)?;
            }
            if let Some(g) = &c.generated {
                b.bind_generated(t, g, c.ty, c.typmod)?;
            }
        }
        for cons in &t.constraints {
            if let ConstraintKind::Check(sql) = &cons.kind {
                b.bind_table_expr(t, sql)?;
            }
        }
        Ok(())
    }

    /// Resolves a column type, reporting whether it was a serial pseudo-type.
    fn column_type(&self, dt: &a::DataType) -> PgResult<(Type, i32, bool)> {
        if let a::DataType::Custom(name, _) = dt {
            let n = name_parts(name).pop().unwrap_or_default();
            let serial = match n.as_str() {
                "serial" | "serial4" => Some(Type::INT4),
                "bigserial" | "serial8" => Some(Type::INT8),
                "smallserial" | "serial2" => Some(Type::INT2),
                _ => None,
            };
            if let Some(t) = serial {
                return Ok((t, -1, true));
            }
        }
        let (db, info) = self.binder();
        let b = Binder::new(&db, &info, &[]);
        let (ty, typmod) = b.data_type(dt)?;
        Ok((ty, typmod, false))
    }

    fn table_constraint(
        &self,
        c: &a::TableConstraint,
    ) -> PgResult<(String, Vec<String>, PendingConstraint)> {
        Ok(match c {
            a::TableConstraint::PrimaryKey(pk) => (
                pk.name.as_ref().map(ident).unwrap_or_default(),
                index_column_names(&pk.columns),
                PendingConstraint::PrimaryKey,
            ),
            a::TableConstraint::Unique(u) => (
                u.name.as_ref().map(ident).unwrap_or_default(),
                index_column_names(&u.columns),
                PendingConstraint::UniqueNulls(!matches!(
                    u.nulls_distinct,
                    a::NullsDistinctOption::NotDistinct
                )),
            ),
            a::TableConstraint::ForeignKey(fk) => (
                fk.name.as_ref().map(ident).unwrap_or_default(),
                fk.columns.iter().map(ident).collect(),
                PendingConstraint::ForeignKey {
                    table: name_parts(&fk.foreign_table),
                    cols: fk.referred_columns.iter().map(ident).collect(),
                    on_delete: fk_action(&fk.on_delete),
                    on_update: fk_action(&fk.on_update),
                },
            ),
            a::TableConstraint::Check(chk) => (
                chk.name.as_ref().map(ident).unwrap_or_default(),
                vec![],
                PendingConstraint::Check(chk.expr.to_string()),
            ),
            other => return Err(unsupported(&format!("table constraint {other}"))),
        })
    }

    fn add_constraint(
        &mut self,
        table: u32,
        name: &str,
        cols: &[String],
        kind: PendingConstraint,
    ) -> PgResult<()> {
        let t = self.ctx.db.table(table).unwrap();
        let tname = t.name.clone();
        let schema = t.schema;
        let mut idxs = vec![];
        for c in cols {
            idxs.push(t.col_index(c).ok_or_else(|| {
                PgError::new(
                    code::UNDEFINED_COLUMN,
                    format!("column \"{c}\" named in key does not exist"),
                )
            })?);
        }
        let (kind, label, nulls_distinct) = match kind {
            PendingConstraint::PrimaryKey => (ConstraintKind::PrimaryKey, "pkey", true),
            PendingConstraint::Unique => (ConstraintKind::Unique, "key", true),
            PendingConstraint::UniqueNulls(nd) => (ConstraintKind::Unique, "key", nd),
            PendingConstraint::Check(sql) => (ConstraintKind::Check(sql), "check", true),
            PendingConstraint::ForeignKey { table: parts, cols: refcols, on_delete, on_update } => {
                let rname = parts.last().cloned().unwrap_or_default();
                let rschema =
                    if parts.len() > 1 { Some(parts[parts.len() - 2].clone()) } else { None };
                let (db, info) = self.binder();
                let b = Binder::new(&db, &info, &[]);
                let ref_oid = b.lookup_table_oid(rschema.as_deref(), &rname)?;
                let parent = self.ctx.db.table(ref_oid).unwrap();
                let ref_idx: Vec<usize> = if refcols.is_empty() {
                    let pk = parent.primary_key().ok_or_else(|| {
                        PgError::new(
                            code::UNDEFINED_OBJECT,
                            format!(
                                "there is no primary key for referenced table \"{}\"",
                                parent.name
                            ),
                        )
                    })?;
                    pk.cols.clone()
                } else {
                    refcols
                        .iter()
                        .map(|c| {
                            parent.col_index(c).ok_or_else(|| {
                                PgError::new(
                                    code::UNDEFINED_COLUMN,
                                    format!("column \"{c}\" referenced in foreign key constraint does not exist"),
                                )
                            })
                        })
                        .collect::<PgResult<_>>()?
                };
                if ref_idx.len() != idxs.len() {
                    return Err(PgError::new(
                        code::INVALID_FOREIGN_KEY,
                        "number of referencing and referenced columns for foreign key disagree",
                    ));
                }
                // The referenced columns must be unique.
                let unique = parent.constraints.iter().any(|c| {
                    matches!(c.kind, ConstraintKind::PrimaryKey | ConstraintKind::Unique)
                        && c.cols == ref_idx
                });
                if !unique {
                    let names: Vec<String> =
                        ref_idx.iter().map(|&i| parent.columns[i].name.clone()).collect();
                    return Err(PgError::new(
                        code::INVALID_FOREIGN_KEY,
                        format!(
                            "there is no unique constraint matching given keys for referenced table \"{}\"",
                            parent.name
                        ),
                    )
                    .detail(format!("Key ({}) is not unique.", names.join(", "))));
                }
                (
                    ConstraintKind::ForeignKey {
                        ref_table: ref_oid,
                        ref_cols: ref_idx,
                        on_delete,
                        on_update,
                    },
                    "fkey",
                    true,
                )
            }
        };
        // Postgres names constraints t_pkey, t_a_b_key, t_a_fkey, t_a_check.
        let colnames: Vec<String> = idxs
            .iter()
            .map(|&i| self.ctx.db.table(table).unwrap().columns[i].name.clone())
            .collect();
        let addition = match label {
            "pkey" => None,
            "key" => Some(colnames.join("_")),
            _ => colnames.first().cloned(),
        };
        let name = if name.is_empty() {
            let base = make_object_name(&tname, addition.as_deref(), label);
            let mut candidate = base.clone();
            let mut i = 1;
            while self.ctx.db.constraint_name_taken(table, &candidate)
                || self.ctx.db.relation_exists(schema, &candidate)
            {
                candidate = format!("{base}{i}");
                i += 1;
            }
            candidate
        } else {
            if self.ctx.db.constraint_name_taken(table, name) {
                return Err(PgError::new(
                    code::DUPLICATE_OBJECT,
                    format!("constraint \"{name}\" for relation \"{tname}\" already exists"),
                ));
            }
            name.to_string()
        };
        let oid = self.ctx.db.alloc_oid();
        let index_oid = matches!(kind, ConstraintKind::PrimaryKey | ConstraintKind::Unique)
            .then(|| self.ctx.db.alloc_oid());
        // A unique/primary constraint creates an index with the same name.
        if let Some(io) = index_oid {
            let t = self.ctx.db.table_mut(table).unwrap();
            t.indexes.push(Index {
                oid: io,
                name: name.clone(),
                cols: idxs.iter().map(|&i| Some(i)).collect(),
                exprs: vec![],
                unique: true,
                primary: matches!(kind, ConstraintKind::PrimaryKey),
                desc: vec![false; idxs.len()],
                predicate: None,
                method: "btree".into(),
                nulls_not_distinct: !nulls_distinct,
            });
        }
        if matches!(kind, ConstraintKind::PrimaryKey) {
            let t = self.ctx.db.table_mut(table).unwrap();
            if t.primary_key().is_some() {
                return Err(PgError::new(
                    code::INVALID_TABLE_DEFINITION,
                    format!("multiple primary keys for table \"{tname}\" are not allowed"),
                ));
            }
            for &i in &idxs {
                t.columns[i].not_null = true;
            }
        }
        let t = self.ctx.db.table_mut(table).unwrap();
        t.constraints.push(Constraint {
            oid,
            name,
            kind,
            cols: idxs,
            index_oid,
            deferrable: false,
            comment: None,
        });
        // Existing rows must satisfy the new constraint.
        let rows = self.ctx.db.table(table).unwrap().rows.clone();
        for (i, r) in rows.iter().enumerate() {
            super::dml::check_row(self.ctx, table, r, Some(i))?;
        }
        Ok(())
    }

    pub fn create_sequence_object(
        &mut self,
        schema: u32,
        name: &str,
        ty: Type,
        owned_by: Option<(u32, usize)>,
    ) -> PgResult<u32> {
        let oid = self.ctx.db.alloc_oid();
        let max = match ty.base {
            Base::Int2 => i16::MAX as i64,
            Base::Int4 => i32::MAX as i64,
            _ => i64::MAX,
        };
        self.ctx.db.sequences.insert(
            oid,
            Sequence {
                oid,
                name: name.to_string(),
                schema,
                ty,
                start: 1,
                increment: 1,
                min: 1,
                max,
                cache: 1,
                cycle: false,
                owned_by,
                comment: None,
            },
        );
        Ok(oid)
    }

    pub fn create_sequence(&mut self, cs: &CreateSequenceStmt) -> PgResult<String> {
        let parts = name_parts(cs.name);
        let (schema, name) = self.target(&parts)?;
        if self.ctx.db.relation_exists(schema, &name) {
            if cs.if_not_exists {
                return Ok("CREATE SEQUENCE".into());
            }
            return Err(PgError::new(
                code::DUPLICATE_TABLE,
                format!("relation \"{name}\" already exists"),
            ));
        }
        let oid = self.create_sequence_object(schema, &name, Type::INT8, None)?;
        let seq = self.ctx.db.sequences.get_mut(&oid).unwrap();
        let mut start: Option<i64> = None;
        for opt in cs.sequence_options {
            match opt {
                a::SequenceOptions::IncrementBy(e, _) => {
                    seq.increment = literal_int(e)?;
                    if seq.increment == 0 {
                        return Err(PgError::new(
                            code::INVALID_PARAMETER_VALUE,
                            "INCREMENT must not be zero",
                        ));
                    }
                }
                a::SequenceOptions::MinValue(v) => match v {
                    Some(e) => seq.min = literal_int(e)?,
                    None => seq.min = if seq.increment > 0 { 1 } else { i64::MIN },
                },
                a::SequenceOptions::MaxValue(v) => match v {
                    Some(e) => seq.max = literal_int(e)?,
                    None => seq.max = if seq.increment > 0 { i64::MAX } else { -1 },
                },
                a::SequenceOptions::StartWith(e, _) => start = Some(literal_int(e)?),
                a::SequenceOptions::Cache(e) => seq.cache = literal_int(e)?,
                a::SequenceOptions::Cycle(no) => seq.cycle = !*no,
            }
        }
        if seq.increment < 0 && seq.min == 1 {
            seq.min = i64::MIN;
            seq.max = -1;
        }
        seq.start = start.unwrap_or(if seq.increment > 0 { seq.min } else { seq.max });
        if let Some(owned) = cs.owned_by {
            let parts = name_parts(owned);
            if parts.len() >= 2 && parts[0] != "none" {
                let col = parts.last().unwrap().clone();
                let tname = parts[parts.len() - 2].clone();
                let (db, info) = self.binder();
                let b = Binder::new(&db, &info, &[]);
                if let Ok(toid) = b.lookup_table_oid(None, &tname)
                    && let Some(idx) = db.table(toid).and_then(|t| t.col_index(&col))
                {
                    self.ctx.db.sequences.get_mut(&oid).unwrap().owned_by = Some((toid, idx));
                }
            }
        }
        Ok("CREATE SEQUENCE".into())
    }

    pub fn create_schema(&mut self, name: &a::SchemaName, if_not_exists: bool) -> PgResult<String> {
        let n = match name {
            a::SchemaName::Simple(n) => name_parts(n).pop().unwrap_or_default(),
            a::SchemaName::UnnamedAuthorization(id) => ident(id),
            a::SchemaName::NamedAuthorization(n, _) => name_parts(n).pop().unwrap_or_default(),
        };
        if self.ctx.db.schema_by_name(&n).is_some() {
            if if_not_exists {
                return Ok("CREATE SCHEMA".into());
            }
            return Err(PgError::new(
                code::DUPLICATE_SCHEMA,
                format!("schema \"{n}\" already exists"),
            ));
        }
        let oid = self.ctx.db.alloc_oid();
        self.ctx
            .db
            .schemas
            .insert(oid, Schema { oid, name: n, owner: BOOTSTRAP_SUPERUSER, comment: None });
        Ok("CREATE SCHEMA".into())
    }

    pub fn create_view(
        &mut self,
        name: &a::ObjectName,
        query: &a::Query,
        columns: &[a::ViewColumnDef],
        or_replace: bool,
        materialized: bool,
    ) -> PgResult<String> {
        let parts = name_parts(name);
        let (schema, vname) = self.target(&parts)?;
        let (db, info) = self.binder();
        let mut b = Binder::new(&db, &info, &[]);
        let (_, cols) = b.bind_query(query)?;
        if let Some(existing) = self.ctx.db.find_table(schema, &vname) {
            if !or_replace {
                return Err(PgError::new(
                    code::DUPLICATE_TABLE,
                    format!("relation \"{vname}\" already exists"),
                ));
            }
            let oid = existing.oid;
            let t = self.ctx.db.table_mut(oid).unwrap();
            t.view_sql = Some(query.to_string());
            t.columns = view_columns(&cols, columns);
            return Ok("CREATE VIEW".into());
        }
        let oid = self.ctx.db.alloc_oid();
        let type_oid = self.ctx.db.alloc_oid();
        let table = Table {
            oid,
            name: vname,
            schema,
            kind: if materialized { RelKind::MaterializedView } else { RelKind::View },
            columns: view_columns(&cols, columns),
            rows: vec![],
            constraints: vec![],
            indexes: vec![],
            view_sql: Some(query.to_string()),
            comment: None,
            type_oid,
            temp: false,
            owner_session: None,
        };
        self.ctx.db.tables.insert(oid, std::sync::Arc::new(table));
        if materialized {
            let (db, info) = self.binder();
            let mut b = Binder::new(&db, &info, &[]);
            let (plan, _) = b.bind_query(query)?;
            let rows = super::exec::run_query(&plan, self.ctx)?;
            self.ctx.db.table_mut(oid).unwrap().rows = rows;
            return Ok("SELECT".into());
        }
        Ok("CREATE VIEW".into())
    }

    pub fn create_index(&mut self, ci: &a::CreateIndex) -> PgResult<String> {
        let parts = name_parts(&ci.table_name);
        let tname = parts.last().cloned().unwrap_or_default();
        let tschema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        let (db, info) = self.binder();
        let b = Binder::new(&db, &info, &[]);
        let oid = b.lookup_table_oid(tschema.as_deref(), &tname)?;
        let t = db.table(oid).unwrap();
        let mut cols = vec![];
        let mut exprs = vec![];
        let mut desc = vec![];
        for c in &ci.columns {
            desc.push(c.column.options.sort == Some(a::OrderBySort::Desc));
            match &c.column.expr {
                a::Expr::Identifier(id) => {
                    let n = ident(id);
                    let idx = t.col_index(&n).ok_or_else(|| {
                        PgError::new(
                            code::UNDEFINED_COLUMN,
                            format!("column \"{n}\" does not exist"),
                        )
                    })?;
                    cols.push(Some(idx));
                }
                other => {
                    let mut b2 = Binder::new(&db, &info, &[]);
                    b2.bind_table_expr(t, &other.to_string())?;
                    cols.push(None);
                    exprs.push(other.to_string());
                }
            }
        }
        let name = match &ci.name {
            Some(n) => name_parts(n).pop().unwrap_or_default(),
            None => {
                let base: Vec<String> = cols
                    .iter()
                    .map(|c| match c {
                        Some(i) => t.columns[*i].name.clone(),
                        None => "expr".into(),
                    })
                    .collect();
                self.ctx.db.unique_rel_name(t.schema, &format!("{}_{}_idx", t.name, base.join("_")))
            }
        };
        if self.ctx.db.relation_exists(t.schema, &name) {
            if ci.if_not_exists {
                return Ok("CREATE INDEX".into());
            }
            return Err(PgError::new(
                code::DUPLICATE_TABLE,
                format!("relation \"{name}\" already exists"),
            ));
        }
        let predicate = ci.predicate.as_ref().map(|p| p.to_string());
        let idx_oid = self.ctx.db.alloc_oid();
        let method = ci
            .using
            .as_ref()
            .map(|u| u.to_string().to_lowercase())
            .unwrap_or_else(|| "btree".into());
        let index = Index {
            oid: idx_oid,
            name,
            cols: cols.clone(),
            exprs,
            unique: ci.unique,
            primary: false,
            desc,
            predicate,
            method,
            nulls_not_distinct: ci.nulls_distinct == Some(false),
        };
        let t = self.ctx.db.table_mut(oid).unwrap();
        t.indexes.push(index);
        // A unique index behaves like a unique constraint.
        if ci.unique && cols.iter().all(|c| c.is_some()) {
            let rows = self.ctx.db.table(oid).unwrap().rows.clone();
            let keys: Vec<usize> = cols.iter().map(|c| c.unwrap()).collect();
            let t = self.ctx.db.table(oid).unwrap();
            for (i, r) in rows.iter().enumerate() {
                if let Some(_dup) = check_unique_violation(t, &keys, r, Some(i), false) {
                    let names: Vec<String> =
                        keys.iter().map(|&k| t.columns[k].name.clone()).collect();
                    let idxname = t.indexes.last().unwrap().name.clone();
                    return Err(PgError::new(
                        code::UNIQUE_VIOLATION,
                        format!("could not create unique index \"{idxname}\""),
                    )
                    .detail(format!("Key ({}) is duplicated.", names.join(", "))));
                }
            }
        }
        Ok("CREATE INDEX".into())
    }

    pub fn create_enum(&mut self, name: &a::ObjectName, labels: &[a::Ident]) -> PgResult<String> {
        let parts = name_parts(name);
        let (schema, n) = self.target(&parts)?;
        if self.ctx.db.find_enum(schema, &n).is_some() {
            return Err(PgError::new(
                code::DUPLICATE_OBJECT,
                format!("type \"{n}\" already exists"),
            ));
        }
        let oid = self.ctx.db.alloc_oid();
        // The array type takes the next OID by convention.
        let _array_oid = self.ctx.db.alloc_oid();
        let mut lbls = vec![];
        for (i, l) in labels.iter().enumerate() {
            let text = l.value.clone();
            let loid = self.ctx.db.alloc_oid();
            lbls.push((i as f32 + 1.0, text, loid));
        }
        self.ctx
            .db
            .enums
            .insert(oid, EnumType { oid, name: n, schema, labels: lbls, comment: None });
        Ok("CREATE TYPE".into())
    }

    pub fn drop(&mut self, d: &a::Statement) -> PgResult<String> {
        let a::Statement::Drop { object_type, if_exists, names, cascade, .. } = d else {
            return Err(unsupported("DROP"));
        };
        for name in names {
            let parts = name_parts(name);
            let n = parts.last().cloned().unwrap_or_default();
            let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
            let found = self.find_object(*object_type, schema.as_deref(), &n)?;
            match found {
                None if *if_exists => continue,
                None => {
                    return Err(match object_type {
                        a::ObjectType::Schema => PgError::new(
                            code::INVALID_SCHEMA_NAME,
                            format!("schema \"{n}\" does not exist"),
                        ),
                        a::ObjectType::Type => PgError::new(
                            code::UNDEFINED_OBJECT,
                            format!("type \"{n}\" does not exist"),
                        ),
                        _ => undefined_table(&n),
                    });
                }
                Some(oid) => self.drop_object(*object_type, oid, *cascade)?,
            }
        }
        Ok(format!("DROP {}", object_kind_name(*object_type)))
    }

    fn find_object(
        &self,
        kind: a::ObjectType,
        schema: Option<&str>,
        name: &str,
    ) -> PgResult<Option<u32>> {
        let schemas: Vec<u32> = match schema {
            Some(s) => match self.ctx.db.schema_by_name(s) {
                Some(o) => vec![o],
                None => return Ok(None),
            },
            None => {
                self.info.search_path.iter().filter_map(|s| self.ctx.db.schema_by_name(s)).collect()
            }
        };
        Ok(match kind {
            a::ObjectType::Schema => self.ctx.db.schema_by_name(name),
            a::ObjectType::Sequence => {
                schemas.iter().find_map(|&s| self.ctx.db.find_sequence(s, name).map(|x| x.oid))
            }
            a::ObjectType::Type => {
                schemas.iter().find_map(|&s| self.ctx.db.find_enum(s, name).map(|x| x.oid))
            }
            a::ObjectType::Index => {
                schemas.iter().find_map(|&s| self.ctx.db.find_index(s, name).map(|(_, i)| i.oid))
            }
            _ => schemas.iter().find_map(|&s| self.ctx.db.find_table(s, name).map(|x| x.oid)),
        })
    }

    fn drop_object(&mut self, kind: a::ObjectType, oid: u32, cascade: bool) -> PgResult<()> {
        match kind {
            a::ObjectType::Schema => {
                let contents: Vec<u32> = self
                    .ctx
                    .db
                    .tables
                    .values()
                    .filter(|t| t.schema == oid)
                    .map(|t| t.oid)
                    .collect();
                if !contents.is_empty() && !cascade {
                    let name = self.ctx.db.schema_name(oid).to_string();
                    return Err(PgError::new(
                        code::DEPENDENT_OBJECTS_STILL_EXIST,
                        format!("cannot drop schema {name} because other objects depend on it"),
                    )
                    .hint("Use DROP ... CASCADE to drop the dependent objects too."));
                }
                for t in contents {
                    self.drop_object(a::ObjectType::Table, t, true)?;
                }
                self.ctx.db.sequences.retain(|_, s| s.schema != oid);
                self.ctx.db.enums.retain(|_, e| e.schema != oid);
                self.ctx.db.schemas.remove(&oid);
            }
            a::ObjectType::Sequence => {
                self.ctx.db.sequences.remove(&oid);
                self.ctx.seqs.remove(&oid);
            }
            a::ObjectType::Type => {
                self.ctx.db.enums.remove(&oid);
            }
            a::ObjectType::Index => {
                for t in self.ctx.db.tables.clone().values() {
                    if t.indexes.iter().any(|i| i.oid == oid) {
                        let tm = self.ctx.db.table_mut(t.oid).unwrap();
                        tm.indexes.retain(|i| i.oid != oid);
                    }
                }
            }
            _ => {
                // Refuse if another table's foreign key points here.
                let name = self.ctx.db.table(oid).map(|t| t.name.clone()).unwrap_or_default();
                let dependents: Vec<(u32, String, String)> = self
                    .ctx
                    .db
                    .tables
                    .values()
                    .filter(|t| t.oid != oid)
                    .flat_map(|t| {
                        t.constraints.iter().filter_map(move |c| match &c.kind {
                            ConstraintKind::ForeignKey { ref_table, .. } if *ref_table == oid => {
                                Some((t.oid, t.name.clone(), c.name.clone()))
                            }
                            _ => None,
                        })
                    })
                    .collect();
                if !dependents.is_empty() && !cascade {
                    let (_, tname, cname) = &dependents[0];
                    return Err(PgError::new(
                        code::DEPENDENT_OBJECTS_STILL_EXIST,
                        format!("cannot drop table {name} because other objects depend on it"),
                    )
                    .detail(format!("constraint {cname} on table {tname} depends on table {name}"))
                    .hint("Use DROP ... CASCADE to drop the dependent objects too."));
                }
                for (dep_oid, _, cname) in dependents {
                    let t = self.ctx.db.table_mut(dep_oid).unwrap();
                    t.constraints.retain(|c| c.name != cname);
                }
                let owned: Vec<u32> = self
                    .ctx
                    .db
                    .sequences
                    .values()
                    .filter(|s| s.owned_by.is_some_and(|(t, _)| t == oid))
                    .map(|s| s.oid)
                    .collect();
                for s in owned {
                    self.ctx.db.sequences.remove(&s);
                    self.ctx.seqs.remove(&s);
                }
                self.ctx.db.tables.remove(&oid);
            }
        }
        Ok(())
    }

    pub fn alter_table(
        &mut self,
        name: &a::ObjectName,
        ops: &[a::AlterTableOperation],
        if_exists: bool,
    ) -> PgResult<String> {
        let parts = name_parts(name);
        let tname = parts.last().cloned().unwrap_or_default();
        let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        let (db, info) = self.binder();
        let b = Binder::new(&db, &info, &[]);
        let oid = match b.lookup_table_oid(schema.as_deref(), &tname) {
            Ok(o) => o,
            Err(e) => {
                if if_exists {
                    return Ok("ALTER TABLE".into());
                }
                return Err(e);
            }
        };
        for op in ops {
            self.alter_op(oid, op)?;
        }
        self.validate_table(oid)?;
        Ok("ALTER TABLE".into())
    }

    fn alter_op(&mut self, oid: u32, op: &a::AlterTableOperation) -> PgResult<()> {
        use a::AlterTableOperation as Op;
        match op {
            Op::AddColumn { column_def, if_not_exists, .. } => {
                let cname = ident(&column_def.name);
                if self.ctx.db.table(oid).unwrap().col_index(&cname).is_some() {
                    if *if_not_exists {
                        return Ok(());
                    }
                    let t = self.ctx.db.table(oid).unwrap();
                    return Err(PgError::new(
                        code::DUPLICATE_COLUMN,
                        format!("column \"{cname}\" of relation \"{}\" already exists", t.name),
                    ));
                }
                let (ty, typmod, serial) = self.column_type(&column_def.data_type)?;
                let mut c = Column { typmod, ..Column::new(&cname, ty) };
                let mut pending = vec![];
                for opt in &column_def.options {
                    match &opt.option {
                        a::ColumnOption::NotNull => c.not_null = true,
                        a::ColumnOption::Default(e) => c.default = Some(e.to_string()),
                        a::ColumnOption::PrimaryKey(_) => {
                            pending.push(PendingConstraint::PrimaryKey)
                        }
                        a::ColumnOption::Unique(_) => pending.push(PendingConstraint::Unique),
                        a::ColumnOption::Check(chk) => {
                            pending.push(PendingConstraint::Check(chk.expr.to_string()))
                        }
                        a::ColumnOption::ForeignKey(fk) => {
                            pending.push(PendingConstraint::ForeignKey {
                                table: name_parts(&fk.foreign_table),
                                cols: fk.referred_columns.iter().map(ident).collect(),
                                on_delete: fk_action(&fk.on_delete),
                                on_update: fk_action(&fk.on_update),
                            });
                        }
                        a::ColumnOption::Generated { generation_expr, .. } => {
                            if let Some(e) = generation_expr {
                                c.generated = Some(e.to_string());
                            } else {
                                c.identity = Some((false, 0));
                            }
                        }
                        _ => {}
                    }
                }
                if serial {
                    c.identity = Some((false, 0));
                    c.not_null = true;
                }
                let default_sql = c.default.clone();
                let has_identity = c.identity.is_some();
                let (ty, typmod) = (c.ty, c.typmod);
                let t = self.ctx.db.table_mut(oid).unwrap();
                t.columns.push(c);
                let idx = t.columns.len() - 1;
                if has_identity {
                    let (schema, tname) = {
                        let t = self.ctx.db.table(oid).unwrap();
                        (t.schema, t.name.clone())
                    };
                    let seq_name = self
                        .ctx
                        .db
                        .unique_rel_name(schema, &make_object_name(&tname, Some(&cname), "seq"));
                    let seq =
                        self.create_sequence_object(schema, &seq_name, ty, Some((oid, idx)))?;
                    let t = self.ctx.db.table_mut(oid).unwrap();
                    t.columns[idx].identity = Some((false, seq));
                }
                // Fill existing rows with the default.
                let value = match (&default_sql, has_identity) {
                    (Some(sql), _) => {
                        let (db, info) = self.binder();
                        let mut b = Binder::new(&db, &info, &[]);
                        let te = b.bind_sql_expr(sql)?;
                        let e =
                            b.coerce(te, ty, typmod, super::casts::CastCtx::Assignment, &cname)?;
                        Some(e)
                    }
                    (None, true) => {
                        let seq = self.ctx.db.table(oid).unwrap().columns[idx].identity.unwrap().1;
                        Some(Expr::Call {
                            name: "nextval",
                            args: vec![Expr::Const(Value::Int(seq as i64))],
                            ty: Type::INT8,
                            arg_tys: vec![Type::of(Base::Regclass)],
                        })
                    }
                    _ => None,
                };
                let nrows = self.ctx.db.table(oid).unwrap().rows.len();
                for i in 0..nrows {
                    let v = match &value {
                        Some(e) => super::exec::eval(e, &[], self.ctx)?,
                        None => Value::Null,
                    };
                    let v = super::casts::cast(
                        v,
                        Type::INT8,
                        ty,
                        typmod,
                        false,
                        &self.info.fmt,
                        self.info.now,
                    )
                    .unwrap_or(Value::Null);
                    let t = self.ctx.db.table_mut(oid).unwrap();
                    t.rows[i].push(v);
                }
                if nrows == 0 {
                    let t = self.ctx.db.table_mut(oid).unwrap();
                    for r in t.rows.iter_mut() {
                        r.push(Value::Null);
                    }
                }
                for p in pending {
                    self.add_constraint(oid, "", std::slice::from_ref(&cname), p)?;
                }
            }
            Op::DropColumn { column_names, if_exists, drop_behavior, .. } => {
                let cascade = matches!(drop_behavior, Some(a::DropBehavior::Cascade));
                for column_name in column_names {
                    let cname = ident(column_name);
                    let t = self.ctx.db.table(oid).unwrap();
                    let Some(idx) = t.col_index(&cname) else {
                        if *if_exists {
                            continue;
                        }
                        return Err(PgError::new(
                            code::UNDEFINED_COLUMN,
                            format!("column \"{cname}\" of relation \"{}\" does not exist", t.name),
                        ));
                    };
                    let used: Vec<String> = t
                        .constraints
                        .iter()
                        .filter(|c| {
                            c.cols.contains(&idx) && !matches!(c.kind, ConstraintKind::Check(_))
                        })
                        .map(|c| c.name.clone())
                        .collect();
                    if !used.is_empty() && !cascade {
                        // Postgres drops constraints that only involve this column.
                    }
                    let t = self.ctx.db.table_mut(oid).unwrap();
                    t.columns[idx].dropped = true;
                    t.columns[idx].not_null = false;
                    t.columns[idx].default = None;
                    t.columns[idx].name = format!("........pg.dropped.{}........", idx + 1);
                    t.constraints.retain(|c| !c.cols.contains(&idx));
                    t.indexes.retain(|i| !i.cols.contains(&Some(idx)));
                    for r in t.rows.iter_mut() {
                        r[idx] = Value::Null;
                    }
                }
            }
            Op::RenameColumn { old_column_name, new_column_name } => {
                let old = ident(old_column_name);
                let new = ident(new_column_name);
                let t = self.ctx.db.table(oid).unwrap();
                let idx = t.col_index(&old).ok_or_else(|| {
                    PgError::new(code::UNDEFINED_COLUMN, format!("column \"{old}\" does not exist"))
                })?;
                if t.col_index(&new).is_some() {
                    return Err(PgError::new(
                        code::DUPLICATE_COLUMN,
                        format!("column \"{new}\" of relation \"{}\" already exists", t.name),
                    ));
                }
                let t = self.ctx.db.table_mut(oid).unwrap();
                t.columns[idx].name = new.clone();
                // Stored expressions refer to columns by name.
                for c in t.columns.iter_mut() {
                    if let Some(d) = &c.default {
                        c.default = Some(rename_ident(d, &old, &new));
                    }
                    if let Some(g) = &c.generated {
                        c.generated = Some(rename_ident(g, &old, &new));
                    }
                }
                for cons in t.constraints.iter_mut() {
                    if let ConstraintKind::Check(sql) = &cons.kind {
                        cons.kind = ConstraintKind::Check(rename_ident(sql, &old, &new));
                    }
                }
                for i in t.indexes.iter_mut() {
                    i.exprs = i.exprs.iter().map(|e| rename_ident(e, &old, &new)).collect();
                    i.predicate = i.predicate.as_ref().map(|p| rename_ident(p, &old, &new));
                }
            }
            Op::RenameTable { table_name } => {
                let obj = match table_name {
                    a::RenameTableNameKind::As(n) | a::RenameTableNameKind::To(n) => n,
                };
                let new = name_parts(obj).pop().unwrap_or_default();
                let schema = self.ctx.db.table(oid).unwrap().schema;
                if self.ctx.db.relation_exists(schema, &new) {
                    return Err(PgError::new(
                        code::DUPLICATE_TABLE,
                        format!("relation \"{new}\" already exists"),
                    ));
                }
                self.ctx.db.table_mut(oid).unwrap().name = new;
            }
            Op::AlterColumn { column_name, op } => {
                let cname = ident(column_name);
                let t = self.ctx.db.table(oid).unwrap();
                let idx = t.col_index(&cname).ok_or_else(|| {
                    PgError::new(
                        code::UNDEFINED_COLUMN,
                        format!("column \"{cname}\" of relation \"{}\" does not exist", t.name),
                    )
                })?;
                match op {
                    a::AlterColumnOperation::SetNotNull => {
                        let nulls = t.rows.iter().any(|r| r[idx].is_null());
                        if nulls {
                            return Err(PgError::new(
                                code::NOT_NULL_VIOLATION,
                                format!(
                                    "column \"{cname}\" of relation \"{}\" contains null values",
                                    t.name
                                ),
                            ));
                        }
                        self.ctx.db.table_mut(oid).unwrap().columns[idx].not_null = true;
                    }
                    a::AlterColumnOperation::DropNotNull => {
                        self.ctx.db.table_mut(oid).unwrap().columns[idx].not_null = false;
                    }
                    a::AlterColumnOperation::SetDefault { value } => {
                        self.ctx.db.table_mut(oid).unwrap().columns[idx].default =
                            Some(value.to_string());
                    }
                    a::AlterColumnOperation::DropDefault => {
                        self.ctx.db.table_mut(oid).unwrap().columns[idx].default = None;
                    }
                    a::AlterColumnOperation::SetDataType { data_type, using, .. } => {
                        let (ty, typmod, _) = self.column_type(data_type)?;
                        let old_ty = t.columns[idx].ty;
                        let rows = t.rows.clone();
                        let mut new_vals = vec![];
                        for r in &rows {
                            let v = match using {
                                Some(u) => {
                                    let (db, info) = self.binder();
                                    let mut b = Binder::new(&db, &info, &[]);
                                    let e = b.bind_table_expr(db.table(oid).unwrap(), &u.to_string())?;
                                    super::exec::eval(&e, r, self.ctx)?
                                }
                                None => super::casts::cast(
                                    r[idx].clone(),
                                    old_ty,
                                    ty,
                                    typmod,
                                    false,
                                    &self.info.fmt,
                                    self.info.now,
                                )
                                .map_err(|_| {
                                    PgError::new(
                                        code::DATATYPE_MISMATCH,
                                        format!(
                                            "column \"{cname}\" cannot be cast automatically to type {}",
                                            ty.display(typmod)
                                        ),
                                    )
                                    .hint("You might need to specify \"USING <expr>\".")
                                })?,
                            };
                            new_vals.push(types::apply_typmod(v, ty, typmod, true)?);
                        }
                        let t = self.ctx.db.table_mut(oid).unwrap();
                        t.columns[idx].ty = ty;
                        t.columns[idx].typmod = typmod;
                        for (r, v) in t.rows.iter_mut().zip(new_vals) {
                            r[idx] = v;
                        }
                    }
                    a::AlterColumnOperation::AddGenerated { .. } => {
                        let (schema, tname) = {
                            let t = self.ctx.db.table(oid).unwrap();
                            (t.schema, t.name.clone())
                        };
                        let ty = self.ctx.db.table(oid).unwrap().columns[idx].ty;
                        let seq_name = self.ctx.db.unique_rel_name(
                            schema,
                            &make_object_name(&tname, Some(&cname), "seq"),
                        );
                        let seq =
                            self.create_sequence_object(schema, &seq_name, ty, Some((oid, idx)))?;
                        let t = self.ctx.db.table_mut(oid).unwrap();
                        t.columns[idx].identity = Some((false, seq));
                        t.columns[idx].not_null = true;
                    }
                }
            }
            Op::AddConstraint { constraint, .. } => {
                let (name, cols, kind) = self.table_constraint(constraint)?;
                self.add_constraint(oid, &name, &cols, kind)?;
            }
            Op::DropConstraint { name, if_exists, .. } => {
                let n = ident(name);
                let t = self.ctx.db.table(oid).unwrap();
                if !t.constraints.iter().any(|c| c.name == n) {
                    if *if_exists {
                        return Ok(());
                    }
                    return Err(PgError::new(
                        code::UNDEFINED_OBJECT,
                        format!("constraint \"{n}\" of relation \"{}\" does not exist", t.name),
                    ));
                }
                let t = self.ctx.db.table_mut(oid).unwrap();
                let idx_oid = t.constraints.iter().find(|c| c.name == n).and_then(|c| c.index_oid);
                t.constraints.retain(|c| c.name != n);
                if let Some(io) = idx_oid {
                    t.indexes.retain(|i| i.oid != io);
                }
            }
            Op::OwnerTo { .. } | Op::EnableRowLevelSecurity | Op::DisableRowLevelSecurity => {}
            other => return Err(unsupported(&format!("ALTER TABLE {other}"))),
        }
        Ok(())
    }

    pub fn truncate(
        &mut self,
        names: &[a::TruncateTableTarget],
        restart: bool,
    ) -> PgResult<String> {
        for n in names {
            let parts = name_parts(&n.name);
            let tname = parts.last().cloned().unwrap_or_default();
            let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
            let (db, info) = self.binder();
            let b = Binder::new(&db, &info, &[]);
            let oid = b.lookup_table_oid(schema.as_deref(), &tname)?;
            self.ctx.db.table_mut(oid).unwrap().rows.clear();
            if restart {
                let seqs: Vec<u32> = self
                    .ctx
                    .db
                    .sequences
                    .values()
                    .filter(|s| s.owned_by.is_some_and(|(t, _)| t == oid))
                    .map(|s| s.oid)
                    .collect();
                for s in seqs {
                    self.ctx.seqs.remove(&s);
                }
            }
        }
        Ok("TRUNCATE TABLE".into())
    }

    pub fn comment(&mut self, c: &a::Statement) -> PgResult<String> {
        let a::Statement::Comment { object_type, object_name, comment, .. } = c else {
            return Err(unsupported("COMMENT"));
        };
        let parts = name_parts(object_name);
        let name = parts.last().cloned().unwrap_or_default();
        let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        match object_type {
            a::CommentObject::Table => {
                let (db, info) = self.binder();
                let b = Binder::new(&db, &info, &[]);
                let oid = b.lookup_table_oid(schema.as_deref(), &name)?;
                self.ctx.db.table_mut(oid).unwrap().comment = comment.clone();
            }
            a::CommentObject::Column => {
                if parts.len() < 2 {
                    return Err(PgError::new(code::SYNTAX_ERROR, "column name must be qualified"));
                }
                let tname = parts[parts.len() - 2].clone();
                let tschema = (parts.len() > 2).then(|| parts[parts.len() - 3].clone());
                let (db, info) = self.binder();
                let b = Binder::new(&db, &info, &[]);
                let oid = b.lookup_table_oid(tschema.as_deref(), &tname)?;
                let idx = self.ctx.db.table(oid).unwrap().col_index(&name).ok_or_else(|| {
                    PgError::new(
                        code::UNDEFINED_COLUMN,
                        format!("column \"{name}\" does not exist"),
                    )
                })?;
                self.ctx.db.table_mut(oid).unwrap().columns[idx].comment = comment.clone();
            }
            a::CommentObject::Schema => {
                let oid = self.ctx.db.schema_by_name(&name).ok_or_else(|| {
                    PgError::new(
                        code::INVALID_SCHEMA_NAME,
                        format!("schema \"{name}\" does not exist"),
                    )
                })?;
                self.ctx.db.schemas.get_mut(&oid).unwrap().comment = comment.clone();
            }
            _ => return Err(unsupported("COMMENT ON this object")),
        }
        Ok("COMMENT".into())
    }
}

/// Column names of an index or constraint column list.
fn index_column_names(cols: &[a::IndexColumn]) -> Vec<String> {
    cols.iter()
        .map(|c| match &c.column.expr {
            a::Expr::Identifier(id) => ident(id),
            other => other.to_string(),
        })
        .collect()
}

/// The parts of `Statement::CreateSequence` the engine passes along.
pub struct CreateSequenceStmt<'a> {
    pub name: &'a a::ObjectName,
    pub if_not_exists: bool,
    pub sequence_options: &'a [a::SequenceOptions],
    pub owned_by: Option<&'a a::ObjectName>,
}

/// Renames a bare identifier in stored SQL, leaving strings and quoted
/// identifiers alone.
fn rename_ident(sql: &str, old: &str, new: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                let quote = b[i];
                out.push(quote as char);
                i += 1;
                while i < b.len() {
                    out.push(b[i] as char);
                    if b[i] == quote {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                if word.eq_ignore_ascii_case(old) { out.push_str(new) } else { out.push_str(word) }
            }
            c => {
                out.push(c as char);
                i += 1;
            }
        }
    }
    out
}

enum PendingConstraint {
    PrimaryKey,
    Unique,
    UniqueNulls(bool),
    Check(String),
    ForeignKey { table: Vec<String>, cols: Vec<String>, on_delete: FkAction, on_update: FkAction },
}

fn fk_action(a: &Option<a::ReferentialAction>) -> FkAction {
    match a {
        Some(sqlparser::ast::ReferentialAction::Cascade) => FkAction::Cascade,
        Some(sqlparser::ast::ReferentialAction::SetNull) => FkAction::SetNull,
        Some(sqlparser::ast::ReferentialAction::SetDefault) => FkAction::SetDefault,
        Some(sqlparser::ast::ReferentialAction::Restrict) => FkAction::Restrict,
        _ => FkAction::NoAction,
    }
}

fn literal_int(e: &a::Expr) -> PgResult<i64> {
    match e {
        a::Expr::Value(v) => match &v.value {
            a::Value::Number(n, _) => {
                n.parse().map_err(|_| PgError::new(code::SYNTAX_ERROR, "invalid integer"))
            }
            _ => Err(PgError::new(code::SYNTAX_ERROR, "expected an integer")),
        },
        a::Expr::UnaryOp { op: a::UnaryOperator::Minus, expr } => Ok(-literal_int(expr)?),
        _ => Err(PgError::new(code::SYNTAX_ERROR, "expected an integer")),
    }
}

fn view_columns(cols: &[super::plan::OutCol], names: &[a::ViewColumnDef]) -> Vec<Column> {
    cols.iter()
        .enumerate()
        .map(|(i, c)| {
            let name = names.get(i).map(|n| ident(&n.name)).unwrap_or_else(|| c.name.clone());
            Column { typmod: c.typmod, ..Column::new(&name, c.ty) }
        })
        .collect()
}

fn object_kind_name(k: a::ObjectType) -> &'static str {
    match k {
        a::ObjectType::Table => "TABLE",
        a::ObjectType::View => "VIEW",
        a::ObjectType::MaterializedView => "MATERIALIZED VIEW",
        a::ObjectType::Index => "INDEX",
        a::ObjectType::Schema => "SCHEMA",
        a::ObjectType::Sequence => "SEQUENCE",
        a::ObjectType::Type => "TYPE",
        a::ObjectType::Role => "ROLE",
        a::ObjectType::Database => "DATABASE",
        _ => "OBJECT",
    }
}
