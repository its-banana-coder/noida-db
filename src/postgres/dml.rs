//! INSERT, UPDATE and DELETE, with constraint checks.

use std::cmp::Ordering;

use super::binder::{Binder, SessionInfo};
use super::catalog::{Constraint, ConstraintKind, FkAction, Row, Table};
use super::error::{PgError, PgResult, code};
use super::exec::{self, Ctx};
use super::plan::{ConflictAction, Dml, Expr, From, Query};
use super::plpgsql::{self, Event};
use super::types::{self, Type, Value};

/// BEFORE ROW triggers for one row: the row to write (a trigger may change
/// it), or None when one returned NULL to skip it.
fn before_row(
    ctx: &mut Ctx,
    table: u32,
    event: Event,
    set_cols: &[usize],
    old: Option<&Row>,
    new: Option<Row>,
) -> PgResult<Option<Row>> {
    let trs = plpgsql::triggers_for(ctx, table, "BEFORE", event, true, set_cols);
    let mut cur = new.or_else(|| old.cloned());
    for tr in &trs {
        let Some(row) = cur else { return Ok(None) };
        let new_arg = if event == Event::Delete { None } else { Some(row.clone()) };
        cur = match plpgsql::fire_row(ctx, tr, event, old, new_arg)? {
            Some(r) => Some(r),
            None => return Ok(None),
        };
    }
    Ok(cur)
}

/// AFTER ROW triggers for the rows a statement wrote, then AFTER STATEMENT.
fn after_rows(
    ctx: &mut Ctx,
    table: u32,
    event: Event,
    set_cols: &[usize],
    rows: &[(Option<Row>, Option<Row>)],
) -> PgResult<()> {
    let trs = plpgsql::triggers_for(ctx, table, "AFTER", event, true, set_cols);
    for (old, new) in rows {
        for tr in &trs {
            plpgsql::fire_row(ctx, tr, event, old.as_ref(), new.clone())?;
        }
    }
    plpgsql::fire_statement(ctx, table, "AFTER", event, set_cols)
}

pub fn run_dml(d: &Dml, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    match d {
        Dml::Insert {
            table,
            cols,
            source,
            defaults,
            on_conflict,
            returning,
            overriding_system,
        } => {
            // Subqueries in RETURNING see the table as it was when the
            // statement started (its snapshot), not the rows it wrote.
            let mut snap = returning_snapshot(returning, ctx);
            let rows = exec::run_query(source, ctx)?;
            // Rows this statement inserted or updated: an ON CONFLICT DO
            // UPDATE may not touch one twice.
            let mut touched: std::collections::HashSet<usize> = Default::default();
            plpgsql::fire_statement(ctx, *table, "BEFORE", Event::Insert, &[])?;
            let mut out = vec![];
            let mut count = 0;
            let mut written: Vec<(Option<Row>, Option<Row>)> = vec![];
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
                    if !*overriding_system {
                        let t = table_of(ctx, *table)?;
                        if let Some((true, _)) = t.columns[c].identity {
                            let name = t.columns[c].name.clone();
                            return Err(PgError::new(
                                code::GENERATED_ALWAYS,
                                format!("cannot insert a non-DEFAULT value into column \"{name}\""),
                            )
                            .detail(format!(
                                "Column \"{name}\" is an identity column defined as GENERATED ALWAYS."
                            ))
                            .hint("Use OVERRIDING SYSTEM VALUE to override."));
                        }
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
                if !plpgsql::triggers_for(ctx, *table, "BEFORE", Event::Insert, true, &[])
                    .is_empty()
                {
                    match before_row(ctx, *table, Event::Insert, &[], None, Some(row))? {
                        Some(r) => row = r,
                        None => continue,
                    }
                    apply_generated(ctx, *table, &mut row)?;
                    coerce_row(ctx, *table, &mut row)?;
                }
                // ON CONFLICT
                if let Some(oc) = on_conflict {
                    let t = table_of(ctx, *table)?;
                    if let Some(idx) = find_conflict(t, &row, oc.target.as_deref()) {
                        match &oc.action {
                            ConflictAction::Nothing => continue,
                            ConflictAction::Update { sets, filter } => {
                                if touched.contains(&idx) {
                                    return Err(PgError::new(
                                        code::CARDINALITY_VIOLATION,
                                        "ON CONFLICT DO UPDATE command cannot affect row a second time",
                                    )
                                    .hint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values."));
                                }
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
                                touched.insert(idx);
                                count += 1;
                                if !returning.is_empty() {
                                    let mut r = with_system(ctx, *table, &updated, idx);
                                    // An updated row's xmax is the updating
                                    // transaction (non-zero).
                                    set_xmax(ctx, *table, &mut r, 1);
                                    out.push(project_snap(returning, &r, ctx, &mut snap)?);
                                }
                                continue;
                            }
                        }
                    }
                }
                check_row(ctx, *table, &row, None)?;
                let t = ctx.db.table_mut(*table).unwrap();
                t.rows.push(row.clone());
                let pos = t.rows.len() - 1;
                touched.insert(pos);
                count += 1;
                if !returning.is_empty() {
                    let r = with_system(ctx, *table, &row, pos);
                    out.push(project_snap(returning, &r, ctx, &mut snap)?);
                }
                written.push((None, Some(row)));
            }
            after_rows(ctx, *table, Event::Insert, &[], &written)?;
            ctx.affected = count;
            Ok(out)
        }
        Dml::Update { table, from, filter, sets, defaults, returning } => {
            // Subqueries in RETURNING see the table as it was when the
            // statement started (its snapshot), not the rows it wrote.
            let mut snap = returning_snapshot(returning, ctx);
            let set_cols: Vec<usize> = sets.iter().map(|(c, _)| *c).collect();
            plpgsql::fire_statement(ctx, *table, "BEFORE", Event::Update, &set_cols)?;
            let t = table_of(ctx, *table)?;
            let base = t.rows.clone();
            let sys_cols: Vec<[Value; 6]> =
                (0..base.len()).map(|i| t.system_col_values(i)).collect();
            let extra = match from {
                Some(f) => exec_from_rows(f, ctx)?,
                None => vec![vec![]],
            };
            let mut updates: Vec<(usize, Row)> = vec![];
            let mut out = vec![];
            for (i, r) in base.iter().enumerate() {
                for e in &extra {
                    let mut row = r.clone();
                    row.extend(sys_cols[i].clone());
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
            if !plpgsql::triggers_for(ctx, *table, "BEFORE", Event::Update, true, &set_cols)
                .is_empty()
            {
                let mut kept = vec![];
                for (i, new) in updates {
                    if let Some(mut r) = before_row(
                        ctx,
                        *table,
                        Event::Update,
                        &set_cols,
                        Some(&base[i]),
                        Some(new),
                    )? {
                        apply_generated(ctx, *table, &mut r)?;
                        coerce_row(ctx, *table, &mut r)?;
                        kept.push((i, r));
                    }
                }
                updates = kept;
            }
            for (i, new) in &updates {
                check_row(ctx, *table, new, Some(*i))?;
                cascade_update(ctx, *table, &base[*i], new)?;
            }
            for (i, new) in &updates {
                let t = ctx.db.table_mut(*table).unwrap();
                t.rows[*i] = new.clone();
                if !returning.is_empty() {
                    let mut r = with_system(ctx, *table, new, *i);
                    set_xmax(ctx, *table, &mut r, 1);
                    out.push(project_snap(returning, &r, ctx, &mut snap)?);
                }
            }
            let written: Vec<(Option<Row>, Option<Row>)> = updates
                .iter()
                .map(|(i, new)| (Some(base[*i].clone()), Some(new.clone())))
                .collect();
            after_rows(ctx, *table, Event::Update, &set_cols, &written)?;
            ctx.affected = updates.len();
            Ok(out)
        }
        Dml::Delete { table, using, filter, returning } => {
            // Subqueries in RETURNING see the table as it was when the
            // statement started (its snapshot), not the rows it wrote.
            let mut snap = returning_snapshot(returning, ctx);
            plpgsql::fire_statement(ctx, *table, "BEFORE", Event::Delete, &[])?;
            let t = table_of(ctx, *table)?;
            let base = t.rows.clone();
            let sys_cols: Vec<[Value; 6]> =
                (0..base.len()).map(|i| t.system_col_values(i)).collect();
            let extra = match using {
                Some(f) => exec_from_rows(f, ctx)?,
                None => vec![vec![]],
            };
            let mut doomed = vec![];
            let mut out = vec![];
            for (i, r) in base.iter().enumerate() {
                for e in &extra {
                    let mut row = r.clone();
                    row.extend(sys_cols[i].clone());
                    row.extend(e.clone());
                    if let Some(f) = filter
                        && !matches!(exec::eval(f, &row, ctx)?, Value::Bool(true))
                    {
                        continue;
                    }
                    doomed.push(i);
                    break;
                }
            }
            // BEFORE DELETE triggers may spare a row (by returning NULL).
            if !plpgsql::triggers_for(ctx, *table, "BEFORE", Event::Delete, true, &[]).is_empty() {
                let mut kept = vec![];
                for i in doomed {
                    if before_row(ctx, *table, Event::Delete, &[], Some(&base[i]), None)?.is_some()
                    {
                        kept.push(i);
                    }
                }
                doomed = kept;
            }
            if !returning.is_empty() {
                for &i in &doomed {
                    let r = with_system(ctx, *table, &base[i], i);
                    out.push(project_snap(returning, &r, ctx, &mut snap)?);
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
            let written: Vec<(Option<Row>, Option<Row>)> =
                doomed.iter().map(|&i| (Some(base[i].clone()), None)).collect();
            after_rows(ctx, *table, Event::Delete, &[], &written)?;
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
    // A lone subquery/VALUES/CTE has no width recorded in the plan, so it
    // is read directly at its full width. Found via testing before a public
    // release: projecting it through the SELECT below cut it to zero
    // columns, so `UPDATE ... FROM (VALUES ...)` and `DELETE ... USING
    // (SELECT ...)` silently matched no rows.
    match f {
        From::Cte(slot) => return Ok(ctx.ctes.get(*slot).cloned().flatten().unwrap_or_default()),
        From::Sub(q) => {
            ctx.outer.push(vec![]);
            let r = exec::run_query(q, ctx);
            ctx.outer.pop();
            return r;
        }
        _ => {}
    }
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

/// A stored row followed by its system column values (as RETURNING sees
/// them).
fn with_system(ctx: &Ctx, table: u32, row: &Row, pos: usize) -> Row {
    let mut r = row.clone();
    if let Ok(t) = table_of(ctx, table) {
        r.extend(t.system_col_values(pos));
    }
    r
}

/// Sets `xmax` in a row built by `with_system`.
fn set_xmax(ctx: &Ctx, table: u32, row: &mut Row, v: i64) {
    if let Ok(t) = table_of(ctx, table) {
        let i = t.columns.len() + 3;
        if i < row.len() {
            row[i] = Value::Int(v);
        }
    }
}

fn returning_snapshot(returning: &[Expr], ctx: &Ctx) -> Option<super::catalog::DbState> {
    let has_sub = returning
        .iter()
        .any(|e| e.contains(&|x| matches!(x, Expr::Sub { .. } | Expr::InSub { .. })));
    has_sub.then(|| ctx.db.clone())
}

fn project_snap(
    exprs: &[Expr],
    row: &Row,
    ctx: &mut Ctx,
    snap: &mut Option<super::catalog::DbState>,
) -> PgResult<Row> {
    let Some(db) = snap.as_mut() else { return project(exprs, row, ctx) };
    std::mem::swap(ctx.db, db);
    let r = project(exprs, row, ctx);
    std::mem::swap(ctx.db, db);
    r
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
        search_path: ctx.rt.settings.lookup_path(&ctx.rt.user),
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
    // The table's definition only: copying its rows for every row checked
    // made bulk INSERT/COPY quadratic. Uniqueness checks read the rows
    // in place below; only a unique index that isn't a constraint's
    // (it may evaluate expressions against the row) needs a full copy.
    let full = table_of(ctx, table)?;
    let needs_rows = full
        .indexes
        .iter()
        .any(|i| i.unique && !full.constraints.iter().any(|c| c.index_oid == Some(i.oid)));
    let t = if needs_rows { full.clone() } else { full.without_rows() };
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
    for idx in t.indexes.iter().filter(|i| i.unique) {
        if t.constraints.iter().any(|c| c.index_oid == Some(idx.oid)) {
            continue;
        }
        if let Some(key) = unique_index_conflict(ctx, &t, idx, row, skip)? {
            let names: Vec<String> = idx
                .cols
                .iter()
                .map(|c| c.map_or_else(|| idx.exprs[0].clone(), |i| t.columns[i].name.clone()))
                .collect();
            let vals: Vec<String> = key
                .iter()
                .zip(&idx.cols)
                .map(|(v, c)| value_text(ctx, v, c.map_or(Type::TEXT, |i| t.columns[i].ty)))
                .collect();
            return Err(PgError::new(
                code::UNIQUE_VIOLATION,
                format!("duplicate key value violates unique constraint \"{}\"", idx.name),
            )
            .detail(format!("Key ({})=({}) already exists.", names.join(", "), vals.join(", ")))
            .table(&schema, &t.name)
            .constraint(&idx.name));
        }
    }
    for cons in &t.constraints {
        match &cons.kind {
            // A deferred key is checked at COMMIT (`check_deferred`).
            ConstraintKind::PrimaryKey | ConstraintKind::Unique if is_deferred(cons, ctx.rt) => {}
            ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
                let nulls_not_distinct = cons
                    .index_oid
                    .and_then(|o| t.indexes.iter().find(|i| i.oid == o))
                    .is_some_and(|i| i.nulls_not_distinct);
                if let Some(other) = super::catalog::check_unique_violation(
                    table_of(ctx, table)?,
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
                // Deferred: checked at COMMIT (`check_deferred`).
                if cons.cols.iter().any(|&c| row[c].is_null()) || is_deferred(cons, ctx.rt) {
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

/// The key `idx` stores for `row`: `None` when a partial index excludes it.
/// An index's predicate and key expressions, bound once.
struct BoundIndex {
    pred: Option<Expr>,
    exprs: Vec<Expr>,
}

fn bind_index(ctx: &mut Ctx, t: &Table, idx: &super::catalog::Index) -> PgResult<BoundIndex> {
    let pred = idx.predicate.as_ref().map(|p| bind_check(ctx, t, p)).transpose()?;
    let exprs = idx.exprs.iter().map(|e| bind_check(ctx, t, e)).collect::<PgResult<_>>()?;
    Ok(BoundIndex { pred, exprs })
}

/// `row`'s key in the index (`None`: outside a partial index's predicate).
fn index_key(
    ctx: &mut Ctx,
    b: &BoundIndex,
    idx: &super::catalog::Index,
    row: &Row,
) -> PgResult<Option<Vec<Value>>> {
    if let Some(e) = &b.pred
        && !matches!(exec::eval(e, row, ctx)?, Value::Bool(true))
    {
        return Ok(None);
    }
    let mut exprs = b.exprs.iter();
    let mut key = vec![];
    for c in &idx.cols {
        match c {
            Some(i) => key.push(row[*i].clone()),
            None => key.push(exec::eval(exprs.next().expect("expression index key"), row, ctx)?),
        }
    }
    Ok(Some(key))
}

/// A key that unique index `idx` can't hold twice (nulls are distinct
/// unless NULLS NOT DISTINCT).
fn unique_key(
    ctx: &mut Ctx,
    b: &BoundIndex,
    idx: &super::catalog::Index,
    row: &Row,
) -> PgResult<Option<Vec<Value>>> {
    Ok(index_key(ctx, b, idx, row)?
        .filter(|k| idx.nulls_not_distinct || !k.iter().any(|v| v.is_null())))
}

/// The row of `t` (other than `skip`) that `row` collides with in the unique
/// index `idx`.
pub fn unique_index_conflict(
    ctx: &mut Ctx,
    t: &Table,
    idx: &super::catalog::Index,
    row: &Row,
    skip: Option<usize>,
) -> PgResult<Option<Vec<Value>>> {
    let b = bind_index(ctx, t, idx)?;
    let Some(key) = unique_key(ctx, &b, idx, row)? else { return Ok(None) };
    for (i, other) in t.rows.iter().enumerate() {
        if Some(i) == skip {
            continue;
        }
        if let Some(k) = index_key(ctx, &b, idx, other)?
            && k.iter().zip(&key).all(|(a, b)| types::values_equal(a, b))
        {
            return Ok(Some(key));
        }
    }
    Ok(None)
}

/// Whether two of `t`'s rows already collide in unique index `idx` (each
/// row's key computed once).
pub fn unique_index_has_duplicate(
    ctx: &mut Ctx,
    t: &Table,
    idx: &super::catalog::Index,
) -> PgResult<bool> {
    let b = bind_index(ctx, t, idx)?;
    let mut keys: Vec<Vec<Value>> = vec![];
    for r in &t.rows {
        if let Some(k) = unique_key(ctx, &b, idx, r)? {
            if keys.iter().any(|o| o.iter().zip(&k).all(|(a, b)| types::values_equal(a, b))) {
                return Ok(true);
            }
            keys.push(k);
        }
    }
    Ok(false)
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
        if is_deferred(&cons, ctx.rt) && matches!(on_delete, FkAction::NoAction) {
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

/// Whether a constraint is deferred now: `SET CONSTRAINTS` for it by
/// name, else `SET CONSTRAINTS ALL`, else its `INITIALLY DEFERRED`.
pub fn is_deferred(cons: &super::catalog::Constraint, rt: &super::exec::Runtime) -> bool {
    if !cons.deferrable && !cons.initially_deferred {
        return false;
    }
    if let Some(&d) = rt.deferred.get(&cons.name) {
        return d;
    }
    rt.deferred_all.unwrap_or(cons.initially_deferred)
}

/// A deferred key, checked over the whole table: sorted on the key, a
/// duplicate sits next to its twin.
fn check_unique_now(
    db: &super::catalog::DbState,
    t: &super::catalog::Table,
    cons: &super::catalog::Constraint,
) -> PgResult<()> {
    let nulls_not_distinct = cons
        .index_oid
        .and_then(|o| t.indexes.iter().find(|i| i.oid == o))
        .is_some_and(|i| i.nulls_not_distinct);
    let mut keyed: Vec<&Row> = t
        .rows
        .iter()
        .filter(|r| nulls_not_distinct || !cons.cols.iter().any(|&c| r[c].is_null()))
        .collect();
    let cmp = |a: &Row, b: &Row| {
        cons.cols
            .iter()
            .map(|&c| types::cmp_values(&a[c], &b[c]))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    };
    keyed.sort_by(|a, b| cmp(a, b));
    let Some(dup) = keyed
        .windows(2)
        .find(|w| cons.cols.iter().all(|&c| types::values_equal(&w[0][c], &w[1][c])))
    else {
        return Ok(());
    };
    let keys: Vec<String> = cons.cols.iter().map(|&c| t.columns[c].name.clone()).collect();
    let vals: Vec<String> = cons
        .cols
        .iter()
        .map(|&c| types::to_text(&dup[0][c], t.columns[c].ty, &Default::default()))
        .collect();
    Err(PgError::new(
        code::UNIQUE_VIOLATION,
        format!("duplicate key value violates unique constraint \"{}\"", cons.name),
    )
    .detail(format!("Key ({})=({}) already exists.", keys.join(", "), vals.join(", ")))
    .table(db.schema_name(t.schema), &t.name)
    .constraint(&cons.name))
}

/// At COMMIT (and `SET CONSTRAINTS ... IMMEDIATE`): every deferrable
/// foreign key (only `names`, when given) must hold for every row
/// (Postgres checks the rows it queued; checking them all gives the same
/// answer).
pub fn check_deferred(db: &super::catalog::DbState) -> PgResult<()> {
    check_deferred_named(db, None)
}

pub fn check_deferred_named(
    db: &super::catalog::DbState,
    names: Option<&[String]>,
) -> PgResult<()> {
    for t in db.tables.values() {
        for cons in &t.constraints {
            if matches!(cons.kind, ConstraintKind::PrimaryKey | ConstraintKind::Unique)
                && (cons.deferrable || cons.initially_deferred)
            {
                if names.is_none_or(|n| n.contains(&cons.name)) {
                    check_unique_now(db, t, cons)?;
                }
                continue;
            }
            let ConstraintKind::ForeignKey { ref_table, ref_cols, .. } = &cons.kind else {
                continue;
            };
            if !cons.deferrable && !cons.initially_deferred {
                continue;
            }
            if names.is_some_and(|n| !n.contains(&cons.name)) {
                continue;
            }
            let Some(parent) = db.tables.get(ref_table) else { continue };
            for row in &t.rows {
                if cons.cols.iter().any(|&c| row[c].is_null()) {
                    continue;
                }
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
                        .map(|&c| types::to_text(&row[c], t.columns[c].ty, &Default::default()))
                        .collect();
                    return Err(PgError::new(
                        code::FOREIGN_KEY_VIOLATION,
                        format!(
                            "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                            t.name, cons.name
                        ),
                    )
                    .detail(format!(
                        "Key ({})=({}) is not present in table \"{}\".",
                        keys.join(", "),
                        vals.join(", "),
                        parent.name
                    ))
                    .constraint(&cons.name));
                }
            }
        }
    }
    Ok(())
}
