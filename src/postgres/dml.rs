//! INSERT, UPDATE and DELETE, with constraint checks.

use std::cmp::Ordering;

use super::binder::{Binder, SessionInfo};
use super::catalog::{Constraint, ConstraintKind, FkAction, Row, Table};
use super::error::{PgError, PgResult, code};
use super::exec::{self, Ctx};
use super::plan::{ConflictAction, Dml, Expr, From, Query};
use super::types::{self, Type, Value};

pub fn run_dml(d: &Dml, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    match d {
        Dml::Insert { table, cols, source, defaults, on_conflict, returning, .. } => {
            let rows = exec::run_query(source, ctx)?;
            let mut out = vec![];
            let mut count = 0;
            for src in rows {
                let t = table_of(ctx, *table)?;
                let ncols = t.columns.len();
                let mut row: Vec<Value> = vec![Value::Null; ncols];
                let mut given = vec![false; ncols];
                for (i, &c) in cols.iter().enumerate() {
                    let v = src.get(i).cloned().unwrap_or(Value::Null);
                    // DEFAULT in a VALUES list.
                    if matches!(source_default(source, i), Some(col) if col == c) && v.is_null() {
                        continue;
                    }
                    row[c] = v;
                    given[c] = true;
                }
                for c in 0..ncols {
                    if !given[c]
                        && let Some(def) = defaults.get(c).and_then(|d| d.as_ref())
                    {
                        row[c] = exec::eval(def, &[], ctx)?;
                    }
                }
                apply_generated(ctx, *table, &mut row)?;
                coerce_row(ctx, *table, &mut row)?;
                // ON CONFLICT
                if let Some(oc) = on_conflict {
                    let t = table_of(ctx, *table)?;
                    if let Some(idx) = find_conflict(t, &row, oc.target.as_deref()) {
                        match &oc.action {
                            ConflictAction::Nothing => continue,
                            ConflictAction::Update { sets, filter } => {
                                let existing = t.rows[idx].clone();
                                let mut combined = existing.clone();
                                combined.extend(row.clone());
                                if let Some(f) = filter
                                    && !matches!(exec::eval(f, &combined, ctx)?, Value::Bool(true))
                                {
                                    continue;
                                }
                                let mut updated = existing.clone();
                                for (c, e) in sets {
                                    updated[*c] = match e {
                                        Expr::Default(col) => default_value(ctx, *table, *col)?,
                                        _ => exec::eval(e, &combined, ctx)?,
                                    };
                                }
                                apply_generated(ctx, *table, &mut updated)?;
                                coerce_row(ctx, *table, &mut updated)?;
                                check_row(ctx, *table, &updated, Some(idx))?;
                                let t = ctx.db.table_mut(*table).unwrap();
                                t.rows[idx] = updated.clone();
                                count += 1;
                                if !returning.is_empty() {
                                    out.push(project(returning, &updated, ctx)?);
                                }
                                continue;
                            }
                        }
                    }
                }
                check_row(ctx, *table, &row, None)?;
                let t = ctx.db.table_mut(*table).unwrap();
                t.rows.push(row.clone());
                count += 1;
                if !returning.is_empty() {
                    out.push(project(returning, &row, ctx)?);
                }
            }
            ctx.affected = count;
            Ok(out)
        }
        Dml::Update { table, from, filter, sets, defaults, returning } => {
            let base = table_of(ctx, *table)?.rows.clone();
            let extra = match from {
                Some(f) => exec_from_rows(f, ctx)?,
                None => vec![vec![]],
            };
            let mut updates: Vec<(usize, Row)> = vec![];
            let mut out = vec![];
            for (i, r) in base.iter().enumerate() {
                for e in &extra {
                    let mut row = r.clone();
                    row.extend(e.clone());
                    if let Some(f) = filter
                        && !matches!(exec::eval(f, &row, ctx)?, Value::Bool(true))
                    {
                        continue;
                    }
                    let mut updated = r.clone();
                    for (c, expr) in sets {
                        updated[*c] = match expr {
                            Expr::Default(col) => defaults
                                .get(*col)
                                .and_then(|d| d.clone())
                                .map(|d| exec::eval(&d, &[], ctx))
                                .transpose()?
                                .unwrap_or(Value::Null),
                            _ => exec::eval(expr, &row, ctx)?,
                        };
                    }
                    apply_generated(ctx, *table, &mut updated)?;
                    coerce_row(ctx, *table, &mut updated)?;
                    updates.push((i, updated));
                    break;
                }
            }
            for (i, new) in &updates {
                check_row(ctx, *table, new, Some(*i))?;
                cascade_update(ctx, *table, &base[*i], new)?;
            }
            for (i, new) in &updates {
                let t = ctx.db.table_mut(*table).unwrap();
                t.rows[*i] = new.clone();
                if !returning.is_empty() {
                    out.push(project(returning, new, ctx)?);
                }
            }
            ctx.affected = updates.len();
            Ok(out)
        }
        Dml::Delete { table, using, filter, returning } => {
            let base = table_of(ctx, *table)?.rows.clone();
            let extra = match using {
                Some(f) => exec_from_rows(f, ctx)?,
                None => vec![vec![]],
            };
            let mut doomed = vec![];
            let mut out = vec![];
            for (i, r) in base.iter().enumerate() {
                for e in &extra {
                    let mut row = r.clone();
                    row.extend(e.clone());
                    if let Some(f) = filter
                        && !matches!(exec::eval(f, &row, ctx)?, Value::Bool(true))
                    {
                        continue;
                    }
                    doomed.push(i);
                    if !returning.is_empty() {
                        out.push(project(returning, r, ctx)?);
                    }
                    break;
                }
            }
            for &i in &doomed {
                cascade_delete(ctx, *table, &base[i])?;
            }
            let t = ctx.db.table_mut(*table).unwrap();
            let mut keep = 0;
            t.rows.retain(|_| {
                let k = keep;
                keep += 1;
                !doomed.contains(&k)
            });
            ctx.affected = doomed.len();
            Ok(out)
        }
    }
}

/// The row an INSERT would collide with, honoring the arbiter columns.
fn find_conflict(t: &Table, row: &Row, target: Option<&[usize]>) -> Option<usize> {
    for c in &t.constraints {
        if !matches!(c.kind, ConstraintKind::PrimaryKey | ConstraintKind::Unique) {
            continue;
        }
        if let Some(cols) = target
            && c.cols != cols
        {
            continue;
        }
        if let Some(i) = super::catalog::check_unique_violation(t, &c.cols, row, None, false) {
            return Some(i);
        }
    }
    target?;
    // A unique index without a constraint can also arbitrate.
    for idx in t.indexes.iter().filter(|i| i.unique) {
        let cols: Vec<usize> = idx.cols.iter().flatten().copied().collect();
        if cols.len() != idx.cols.len() {
            continue;
        }
        if target.is_some_and(|t| t != cols) {
            continue;
        }
        if let Some(i) =
            super::catalog::check_unique_violation(t, &cols, row, None, idx.nulls_not_distinct)
        {
            return Some(i);
        }
    }
    None
}

fn exec_from_rows(f: &From, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    // The executor's FROM is private; run it through a trivial SELECT.
    let sel = super::plan::Select {
        from: f.clone(),
        filter: None,
        group: None,
        grouping_sets: None,
        aggs: vec![],
        having: None,
        windows: vec![],
        proj: (0..from_width(f)).map(Expr::Col).collect(),
        visible: from_width(f),
        distinct: super::plan::Distinct::None,
        order: vec![],
        limit: None,
        offset: None,
        with_ties: false,
        srf: false,
    };
    exec::run_query(&Query::Select(Box::new(sel)), ctx)
}

fn from_width(f: &From) -> usize {
    match f {
        From::Table { ncols, .. } | From::Virtual { ncols, .. } => *ncols,
        From::Func { ncols, ordinality, .. } => ncols + usize::from(*ordinality),
        From::Join { left_cols, right_cols, .. } => left_cols + right_cols,
        From::One => 0,
        From::Sub(_) | From::Cte(_) => 0,
    }
}

fn source_default(source: &Query, idx: usize) -> Option<usize> {
    if let Query::Values { rows, .. } = source {
        for r in rows {
            if let Some(Expr::Default(c)) = r.get(idx) {
                return Some(*c);
            }
        }
    }
    None
}

fn table_of<'a>(ctx: &'a Ctx, oid: u32) -> PgResult<&'a Table> {
    ctx.db.table(oid).ok_or_else(|| {
        PgError::new(code::UNDEFINED_TABLE, format!("relation with OID {oid} does not exist"))
    })
}

fn project(exprs: &[Expr], row: &Row, ctx: &mut Ctx) -> PgResult<Row> {
    let mut out = vec![];
    for e in exprs {
        out.push(exec::eval(e, row, ctx)?);
    }
    Ok(out)
}

fn default_value(ctx: &mut Ctx, table: u32, col: usize) -> PgResult<Value> {
    let t = table_of(ctx, table)?;
    let Some(sql) = t.columns[col].default.clone() else { return Ok(Value::Null) };
    let (ty, typmod) = (t.columns[col].ty, t.columns[col].typmod);
    let info = session_info(ctx);
    let db = ctx.db.clone();
    let mut b = Binder::new(&db, &info, &[]);
    let te = b.bind_sql_expr(&sql)?;
    let e = b.coerce(te, ty, typmod, super::casts::CastCtx::Assignment, "DEFAULT")?;
    exec::eval(&e, &[], ctx)
}

pub fn session_info(ctx: &Ctx) -> SessionInfo {
    SessionInfo {
        user: ctx.rt.user.clone(),
        database: ctx.rt.database.clone(),
        search_path: ctx.rt.settings.search_path(&ctx.rt.user),
        fmt: ctx.rt.settings.fmt(),
        now: ctx.rt.now,
    }
}

/// Applies typmods and rejects values that don't fit the column types.
fn coerce_row(ctx: &mut Ctx, table: u32, row: &mut Row) -> PgResult<()> {
    let t = table_of(ctx, table)?;
    let specs: Vec<(Type, i32)> = t.columns.iter().map(|c| (c.ty, c.typmod)).collect();
    for (i, (ty, typmod)) in specs.iter().enumerate() {
        let v = std::mem::replace(&mut row[i], Value::Null);
        row[i] = types::apply_typmod(v, *ty, *typmod, false)?;
    }
    Ok(())
}

fn apply_generated(ctx: &mut Ctx, table: u32, row: &mut Row) -> PgResult<()> {
    let t = table_of(ctx, table)?;
    let gens: Vec<(usize, String, Type, i32)> = t
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.generated.clone().map(|g| (i, g, c.ty, c.typmod)))
        .collect();
    if gens.is_empty() {
        return Ok(());
    }
    let info = session_info(ctx);
    let db = ctx.db.clone();
    for (i, sql, ty, typmod) in gens {
        let t = db.table(table).unwrap();
        let mut b = Binder::new(&db, &info, &[]);
        let e = b.bind_generated(t, &sql, ty, typmod)?;
        row[i] = exec::eval(&e, row, ctx)?;
    }
    Ok(())
}

/// Checks NOT NULL, unique/primary key, CHECK and foreign keys.
pub fn check_row(ctx: &mut Ctx, table: u32, row: &Row, skip: Option<usize>) -> PgResult<()> {
    let t = table_of(ctx, table)?.clone();
    let schema = ctx.db.schema_name(t.schema).to_string();
    for (i, c) in t.live_columns() {
        if c.not_null && row[i].is_null() {
            return Err(PgError::new(
                code::NOT_NULL_VIOLATION,
                format!(
                    "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                    c.name, t.name
                ),
            )
            .detail(format!("Failing row contains ({}).", failing_row(ctx, &t, row)))
            .table(&schema, &t.name)
            .column(&c.name));
        }
    }
    for cons in &t.constraints {
        match &cons.kind {
            ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
                let nulls_not_distinct = cons
                    .index_oid
                    .and_then(|o| t.indexes.iter().find(|i| i.oid == o))
                    .is_some_and(|i| i.nulls_not_distinct);
                if let Some(other) = super::catalog::check_unique_violation(
                    &t,
                    &cons.cols,
                    row,
                    skip,
                    nulls_not_distinct,
                ) {
                    let _ = other;
                    let keys: Vec<String> =
                        cons.cols.iter().map(|&c| t.columns[c].name.clone()).collect();
                    let vals: Vec<String> = cons
                        .cols
                        .iter()
                        .map(|&c| value_text(ctx, &row[c], t.columns[c].ty))
                        .collect();
                    return Err(PgError::new(
                        code::UNIQUE_VIOLATION,
                        format!("duplicate key value violates unique constraint \"{}\"", cons.name),
                    )
                    .detail(format!(
                        "Key ({})=({}) already exists.",
                        keys.join(", "),
                        vals.join(", ")
                    ))
                    .table(&schema, &t.name)
                    .constraint(&cons.name));
                }
            }
            ConstraintKind::Check(sql) => {
                let e = bind_check(ctx, &t, sql)?;
                let v = exec::eval(&e, row, ctx)?;
                if matches!(v, Value::Bool(false)) {
                    return Err(PgError::new(
                        code::CHECK_VIOLATION,
                        format!(
                            "new row for relation \"{}\" violates check constraint \"{}\"",
                            t.name, cons.name
                        ),
                    )
                    .detail(format!("Failing row contains ({}).", failing_row(ctx, &t, row)))
                    .table(&schema, &t.name)
                    .constraint(&cons.name));
                }
            }
            ConstraintKind::ForeignKey { ref_table, ref_cols, .. } => {
                if cons.cols.iter().any(|&c| row[c].is_null()) {
                    continue;
                }
                let parent = table_of(ctx, *ref_table)?;
                let found = parent.rows.iter().any(|pr| {
                    cons.cols
                        .iter()
                        .zip(ref_cols)
                        .all(|(&c, &p)| types::cmp_values(&row[c], &pr[p]) == Ordering::Equal)
                });
                if !found {
                    let keys: Vec<String> =
                        cons.cols.iter().map(|&c| t.columns[c].name.clone()).collect();
                    let vals: Vec<String> = cons
                        .cols
                        .iter()
                        .map(|&c| value_text(ctx, &row[c], t.columns[c].ty))
                        .collect();
                    let pname = parent.name.clone();
                    return Err(PgError::new(
                        code::FOREIGN_KEY_VIOLATION,
                        format!(
                            "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                            t.name, cons.name
                        ),
                    )
                    .detail(format!("Key ({})=({}) is not present in table \"{pname}\".", keys.join(", "), vals.join(", ")))
                    .table(&schema, &t.name)
                    .constraint(&cons.name));
                }
            }
        }
    }
    Ok(())
}

fn bind_check(ctx: &mut Ctx, t: &Table, sql: &str) -> PgResult<Expr> {
    let info = session_info(ctx);
    let db = ctx.db.clone();
    let mut b = Binder::new(&db, &info, &[]);
    b.bind_table_expr(t, sql)
}

fn value_text(ctx: &Ctx, v: &Value, ty: Type) -> String {
    if v.is_null() {
        return "null".into();
    }
    let fmt = ctx.rt.settings.fmt();
    types::to_text(v, ty, &fmt)
}

fn failing_row(ctx: &Ctx, t: &Table, row: &Row) -> String {
    t.live_columns()
        .map(|(i, c)| {
            if row[i].is_null() {
                "null".to_string()
            } else {
                types::to_text(&row[i], c.ty, &ctx.rt.settings.fmt())
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Enforces ON DELETE actions of foreign keys pointing at this table.
fn cascade_delete(ctx: &mut Ctx, table: u32, row: &Row) -> PgResult<()> {
    let refs = referencing(ctx, table);
    for (child_oid, cons) in refs {
        let ConstraintKind::ForeignKey { ref_cols, on_delete, .. } = &cons.kind else { continue };
        let child = table_of(ctx, child_oid)?.clone();
        let matching: Vec<usize> = child
            .rows
            .iter()
            .enumerate()
            .filter(|(_, cr)| {
                cons.cols.iter().zip(ref_cols).all(|(&c, &p)| {
                    !cr[c].is_null() && types::cmp_values(&cr[c], &row[p]) == Ordering::Equal
                })
            })
            .map(|(i, _)| i)
            .collect();
        if matching.is_empty() {
            continue;
        }
        match on_delete {
            FkAction::NoAction | FkAction::Restrict => {
                let schema = ctx.db.schema_name(child.schema).to_string();
                let keys: Vec<String> =
                    cons.cols.iter().map(|&c| child.columns[c].name.clone()).collect();
                let vals: Vec<String> = cons
                    .cols
                    .iter()
                    .map(|&c| value_text(ctx, &child.rows[matching[0]][c], child.columns[c].ty))
                    .collect();
                let parent = table_of(ctx, table)?.name.clone();
                return Err(PgError::new(
                    code::FOREIGN_KEY_VIOLATION,
                    format!(
                        "update or delete on table \"{parent}\" violates foreign key constraint \"{}\" on table \"{}\"",
                        cons.name, child.name
                    ),
                )
                .detail(format!(
                    "Key ({})=({}) is still referenced from table \"{}\".",
                    keys.join(", "),
                    vals.join(", "),
                    child.name
                ))
                .table(&schema, &child.name)
                .constraint(&cons.name));
            }
            FkAction::Cascade => {
                let doomed: Vec<Row> = matching.iter().map(|&i| child.rows[i].clone()).collect();
                for d in &doomed {
                    cascade_delete(ctx, child_oid, d)?;
                }
                let t = ctx.db.table_mut(child_oid).unwrap();
                t.rows.retain(|r| {
                    !doomed.iter().any(|d| {
                        r.iter().zip(d).all(|(a, b)| types::cmp_values(a, b) == Ordering::Equal)
                    })
                });
            }
            FkAction::SetNull | FkAction::SetDefault => {
                let set_default = *on_delete == FkAction::SetDefault;
                for i in matching {
                    let mut new = child.rows[i].clone();
                    for &c in &cons.cols {
                        new[c] = if set_default {
                            default_value(ctx, child_oid, c)?
                        } else {
                            Value::Null
                        };
                    }
                    let t = ctx.db.table_mut(child_oid).unwrap();
                    t.rows[i] = new;
                }
            }
        }
    }
    Ok(())
}

/// Enforces ON UPDATE actions when a referenced key changes.
fn cascade_update(ctx: &mut Ctx, table: u32, old: &Row, new: &Row) -> PgResult<()> {
    let refs = referencing(ctx, table);
    for (child_oid, cons) in refs {
        let ConstraintKind::ForeignKey { ref_cols, on_update, .. } = &cons.kind else { continue };
        if ref_cols.iter().all(|&p| types::cmp_values(&old[p], &new[p]) == Ordering::Equal) {
            continue;
        }
        let child = table_of(ctx, child_oid)?.clone();
        let matching: Vec<usize> = child
            .rows
            .iter()
            .enumerate()
            .filter(|(_, cr)| {
                cons.cols.iter().zip(ref_cols).all(|(&c, &p)| {
                    !cr[c].is_null() && types::cmp_values(&cr[c], &old[p]) == Ordering::Equal
                })
            })
            .map(|(i, _)| i)
            .collect();
        if matching.is_empty() {
            continue;
        }
        match on_update {
            FkAction::Cascade => {
                for i in matching {
                    let mut r = child.rows[i].clone();
                    for (&c, &p) in cons.cols.iter().zip(ref_cols) {
                        r[c] = new[p].clone();
                    }
                    let t = ctx.db.table_mut(child_oid).unwrap();
                    t.rows[i] = r;
                }
            }
            FkAction::SetNull | FkAction::SetDefault => {
                let set_default = *on_update == FkAction::SetDefault;
                for i in matching {
                    let mut r = child.rows[i].clone();
                    for &c in &cons.cols {
                        r[c] = if set_default {
                            default_value(ctx, child_oid, c)?
                        } else {
                            Value::Null
                        };
                    }
                    let t = ctx.db.table_mut(child_oid).unwrap();
                    t.rows[i] = r;
                }
            }
            _ => {
                let parent = table_of(ctx, table)?.name.clone();
                let schema = ctx.db.schema_name(child.schema).to_string();
                return Err(PgError::new(
                    code::FOREIGN_KEY_VIOLATION,
                    format!(
                        "update or delete on table \"{parent}\" violates foreign key constraint \"{}\" on table \"{}\"",
                        cons.name, child.name
                    ),
                )
                .table(&schema, &child.name)
                .constraint(&cons.name));
            }
        }
    }
    Ok(())
}

fn referencing(ctx: &Ctx, table: u32) -> Vec<(u32, Constraint)> {
    let mut out = vec![];
    for t in ctx.db.tables.values() {
        for c in &t.constraints {
            if let ConstraintKind::ForeignKey { ref_table, .. } = &c.kind
                && *ref_table == table
            {
                out.push((t.oid, c.clone()));
            }
        }
    }
    out
}
