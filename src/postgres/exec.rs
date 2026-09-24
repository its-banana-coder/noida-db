//! The executor: naive, materializing, single-threaded. Correctness first.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::casts;
use super::catalog::{DbState, Row, SeqValue};
use super::error::{PgError, PgResult, code};
use super::funcs::{self, Env};
use super::json::Json;
use super::numeric::Numeric;
use super::pgcatalog;
use super::plan::*;
use super::session::Settings;
use super::types::{self, Array, Base, Type, Value};

/// Per-connection runtime state the executor and system functions need.
pub struct Runtime {
    pub pid: i32,
    pub user: String,
    pub database: String,
    pub settings: Settings,
    /// Sequence values by sequence OID (never rolled back).
    pub currval: BTreeMap<u32, i64>,
    pub lastval: Option<u32>,
    /// Transaction start and statement start, in Postgres microseconds.
    pub now: i64,
    pub stmt_now: i64,
    /// Channels this session listens on.
    pub listening: Vec<String>,
    /// Notifications to deliver to this session.
    pub notices: Vec<PgError>,
}

pub struct Ctx<'a> {
    pub db: &'a mut DbState,
    pub seqs: &'a mut BTreeMap<u32, SeqValue>,
    pub rt: &'a mut Runtime,
    pub params: &'a [Value],
    /// Rows of enclosing queries, innermost last.
    pub outer: Vec<Row>,
    pub ctes: Vec<Option<Vec<Row>>>,
    /// Pending NOTIFYs (channel, payload) raised by this statement.
    pub notifies: Vec<(String, String)>,
    /// Rows affected by the last DML node.
    pub affected: usize,
}

impl Ctx<'_> {
    /// An [`Env`] borrows its FmtCtx, so hand the caller the parts to own.
    pub fn make_env(&self) -> (types::FmtCtx, i64, i64) {
        (self.rt.settings.fmt(), self.rt.now, self.rt.stmt_now)
    }
}

macro_rules! env {
    ($ctx:expr) => {{
        let (fmt, now, stmt_now) = $ctx.make_env();
        (fmt, now, stmt_now)
    }};
}

/// Evaluates an expression against `row`.
pub fn eval(e: &Expr, row: &[Value], ctx: &mut Ctx) -> PgResult<Value> {
    Ok(match e {
        Expr::Const(v) => v.clone(),
        Expr::Param(i) => ctx.params.get(*i).cloned().unwrap_or(Value::Null),
        Expr::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
        Expr::Outer(depth, i) => {
            let n = ctx.outer.len();
            ctx.outer
                .get(n.wrapping_sub(*depth))
                .and_then(|r| r.get(*i))
                .cloned()
                .unwrap_or(Value::Null)
        }
        Expr::Default(_) => Value::Null,
        Expr::Cast { expr, from, to, typmod, explicit } => {
            let v = eval(expr, row, ctx)?;
            if to.is_reg()
                && let Value::Text(s) = &v
            {
                let path = ctx.rt.settings.search_path(&ctx.rt.user);
                let oid = pgcatalog::resolve_reg(ctx.db, &path, &ctx.rt.user, to.base, s)
                    .ok_or_else(|| pgcatalog::undefined_reg(to.base, s))?;
                return Ok(Value::Int(oid));
            }
            if from.is_reg()
                && (to.is_string() || to.base == Base::Text)
                && let Value::Int(oid) = &v
            {
                let names = reg_names(ctx);
                let text = names
                    .lookup(from.base, *oid as u32)
                    .cloned()
                    .unwrap_or_else(|| oid.to_string());
                return Ok(Value::Text(text));
            }
            let (fmt, now, _) = env!(ctx);
            casts::cast(v, *from, *to, *typmod, *explicit, &fmt, now)?
        }
        Expr::And(items) => {
            let mut any_null = false;
            for i in items {
                match eval(i, row, ctx)? {
                    Value::Bool(false) => return Ok(Value::Bool(false)),
                    Value::Null => any_null = true,
                    _ => {}
                }
            }
            if any_null { Value::Null } else { Value::Bool(true) }
        }
        Expr::Or(items) => {
            let mut any_null = false;
            for i in items {
                match eval(i, row, ctx)? {
                    Value::Bool(true) => return Ok(Value::Bool(true)),
                    Value::Null => any_null = true,
                    _ => {}
                }
            }
            if any_null { Value::Null } else { Value::Bool(false) }
        }
        Expr::Not(x) => match eval(x, row, ctx)? {
            Value::Bool(b) => Value::Bool(!b),
            _ => Value::Null,
        },
        Expr::IsNull(x, negated) => {
            let v = eval(x, row, ctx)?;
            Value::Bool(v.is_null() != *negated)
        }
        Expr::IsBool(x, want, negated) => {
            let v = eval(x, row, ctx)?;
            let matches = match (want, &v) {
                (None, Value::Null) => true,
                (None, _) => false,
                (_, Value::Null) => false,
                (Some(w), Value::Bool(b)) => w == b,
                _ => false,
            };
            Value::Bool(matches != *negated)
        }
        Expr::Compare { op, left, right, bpchar } => {
            let l = eval(left, row, ctx)?;
            let r = eval(right, row, ctx)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            Value::Bool(op.test(compare_values(&l, &r, *bpchar)))
        }
        Expr::Distinct { left, right, negated } => {
            let l = eval(left, row, ctx)?;
            let r = eval(right, row, ctx)?;
            let distinct = match (l.is_null(), r.is_null()) {
                (true, true) => false,
                (true, false) | (false, true) => true,
                _ => types::cmp_values(&l, &r) != Ordering::Equal,
            };
            Value::Bool(distinct != *negated)
        }
        Expr::Case { operand: _, whens, else_ } => {
            for (cond, res) in whens {
                if matches!(eval(cond, row, ctx)?, Value::Bool(true)) {
                    return eval(res, row, ctx);
                }
            }
            match else_ {
                Some(e) => eval(e, row, ctx)?,
                None => Value::Null,
            }
        }
        Expr::Coalesce(items) => {
            for i in items {
                let v = eval(i, row, ctx)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Value::Null
        }
        Expr::NullIf(a, b) => {
            let x = eval(a, row, ctx)?;
            let y = eval(b, row, ctx)?;
            if !x.is_null() && !y.is_null() && types::cmp_values(&x, &y) == Ordering::Equal {
                Value::Null
            } else {
                x
            }
        }
        Expr::Greatest(items, least) => {
            let mut best: Option<Value> = None;
            for i in items {
                let v = eval(i, row, ctx)?;
                if v.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        let take = if *least {
                            types::cmp_values(&v, &b) == Ordering::Less
                        } else {
                            types::cmp_values(&v, &b) == Ordering::Greater
                        };
                        if take { v } else { b }
                    }
                });
            }
            best.unwrap_or(Value::Null)
        }
        Expr::InList { expr, list, negated } => {
            let v = eval(expr, row, ctx)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            let mut saw_null = false;
            for i in list {
                let x = eval(i, row, ctx)?;
                if x.is_null() {
                    saw_null = true;
                    continue;
                }
                if types::cmp_values(&v, &x) == Ordering::Equal {
                    return Ok(Value::Bool(!*negated));
                }
            }
            if saw_null { Value::Null } else { Value::Bool(*negated) }
        }
        Expr::AnyAll { left, op, right, all } => {
            let l = eval(left, row, ctx)?;
            let r = eval(right, row, ctx)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            let items: Vec<Value> = match r {
                Value::Array(a) => a.items,
                other => vec![other],
            };
            let mut saw_null = false;
            for x in items {
                if x.is_null() {
                    saw_null = true;
                    continue;
                }
                let hit = op.test(compare_values(&l, &x, false));
                if *all && !hit {
                    return Ok(Value::Bool(false));
                }
                if !*all && hit {
                    return Ok(Value::Bool(true));
                }
            }
            if saw_null { Value::Null } else { Value::Bool(*all) }
        }
        Expr::InSub { left, op, query, all, negated } => {
            let mut lvals = vec![];
            for l in left {
                lvals.push(eval(l, row, ctx)?);
            }
            let rows = run_subquery(query, row, ctx)?;
            let mut saw_null = false;
            let mut found = false;
            for r in &rows {
                let mut all_eq = true;
                let mut null_here = false;
                for (i, lv) in lvals.iter().enumerate() {
                    let rv = r.get(i).cloned().unwrap_or(Value::Null);
                    if lv.is_null() || rv.is_null() {
                        null_here = true;
                        continue;
                    }
                    if !op.test(compare_values(lv, &rv, false)) {
                        all_eq = false;
                    }
                }
                if null_here && all_eq {
                    saw_null = true;
                } else if all_eq {
                    found = true;
                    if !*all {
                        break;
                    }
                } else if *all {
                    return Ok(Value::Bool(*negated));
                }
            }
            // NOT IN uses `all` as the negation marker (<> ALL).
            if *negated {
                if found {
                    Value::Bool(false)
                } else if saw_null {
                    Value::Null
                } else {
                    Value::Bool(true)
                }
            } else if found {
                Value::Bool(true)
            } else if saw_null {
                Value::Null
            } else {
                Value::Bool(*all)
            }
        }
        Expr::Sub { kind, query } => {
            let rows = run_subquery(query, row, ctx)?;
            match kind {
                SubKind::Exists => Value::Bool(!rows.is_empty()),
                SubKind::Array => Value::Array(Box::new(Array::new(
                    rows.into_iter()
                        .map(|mut r| if r.is_empty() { Value::Null } else { r.remove(0) })
                        .collect(),
                ))),
                SubKind::Scalar => {
                    if rows.len() > 1 {
                        return Err(PgError::new(
                            code::CARDINALITY_VIOLATION,
                            "more than one row returned by a subquery used as an expression",
                        ));
                    }
                    rows.into_iter()
                        .next()
                        .map(|mut r| if r.is_empty() { Value::Null } else { r.remove(0) })
                        .unwrap_or(Value::Null)
                }
            }
        }
        Expr::Array(items) => {
            let mut out = vec![];
            let mut dims: Option<Vec<(i32, i32)>> = None;
            let mut nested = false;
            for i in items {
                let v = eval(i, row, ctx)?;
                match v {
                    Value::Array(a) => {
                        nested = true;
                        match &dims {
                            None => dims = Some(a.dims.clone()),
                            Some(d) if *d != a.dims => {
                                return Err(PgError::new(
                                    code::ARRAY_SUBSCRIPT_ERROR,
                                    "multidimensional arrays must have array expressions with matching dimensions",
                                ));
                            }
                            _ => {}
                        }
                        out.extend(a.items);
                    }
                    other => out.push(other),
                }
            }
            if nested {
                let inner = dims.unwrap_or_default();
                let mut d = vec![(items.len() as i32, 1)];
                d.extend(inner);
                Value::Array(Box::new(Array { dims: d, items: out }))
            } else {
                Value::Array(Box::new(Array::new(out)))
            }
        }
        Expr::Row(items) => {
            let mut out = vec![];
            for i in items {
                out.push(eval(i, row, ctx)?);
            }
            Value::Record(out)
        }
        Expr::Call { name, args, ty, arg_tys } => {
            return call_function(name, args, arg_tys, *ty, row, ctx);
        }
        Expr::AggRef(_) | Expr::WinRef(_) => {
            return Err(PgError::new(
                code::INTERNAL_ERROR,
                "aggregate reference outside an aggregate query",
            ));
        }
    })
}

/// Comparison with bpchar's trailing-space rule.
fn compare_values(a: &Value, b: &Value, bpchar: bool) -> Ordering {
    if bpchar && let (Value::Text(x), Value::Text(y)) = (a, b) {
        return x.trim_end_matches(' ').as_bytes().cmp(y.trim_end_matches(' ').as_bytes());
    }
    types::cmp_values(a, b)
}

fn run_subquery(q: &Query, row: &[Value], ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    ctx.outer.push(row.to_vec());
    let r = run_query(q, ctx);
    ctx.outer.pop();
    r
}

/// Names for reg* values, built from the live catalog.
fn reg_names(ctx: &Ctx) -> types::RegNames {
    let mut r = types::RegNames::default();
    for t in ctx.db.tables.values() {
        r.class.insert(t.oid, t.name.clone());
        for i in &t.indexes {
            r.class.insert(i.oid, i.name.clone());
        }
    }
    for s in ctx.db.sequences.values() {
        r.class.insert(s.oid, s.name.clone());
    }
    for ti in types::TYPES {
        r.types.insert(ti.oid, Type::of(ti.base).display(-1));
        if ti.array_oid != 0 {
            r.types.insert(ti.array_oid, Type::array_of(ti.base).display(-1));
        }
    }
    for e in ctx.db.enums.values() {
        r.types.insert(e.oid, e.name.clone());
    }
    for sig in super::sigs::all_sigs() {
        r.procs.insert(sig.oid, sig.name.to_string());
    }
    for s in ctx.db.schemas.values() {
        r.namespaces.insert(s.oid, s.name.clone());
    }
    r.roles.insert(10, ctx.rt.user.clone());
    r
}

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

// ---------------------------------------------------------------------------
// Function dispatch

fn call_function(
    name: &str,
    args: &[Expr],
    arg_tys: &[Type],
    ret: Type,
    row: &[Value],
    ctx: &mut Ctx,
) -> PgResult<Value> {
    // Operators and functions evaluate their arguments first.
    let mut vals = Vec::with_capacity(args.len());
    for a in args {
        vals.push(eval(a, row, ctx)?);
    }
    let (fmt, now, stmt_now) = env!(ctx);
    let env = Env { fmt: &fmt, now, stmt_now };
    // Operators: two operands, non-alphabetic name.
    if !name.chars().next().is_some_and(|c| c.is_alphabetic()) {
        if let Some(u) = name.strip_suffix('u') {
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            return funcs::unop(u, &vals[0], ret);
        }
        if vals.iter().any(Value::is_null) {
            return Ok(Value::Null);
        }
        return funcs::binop(name, &vals[0], &vals[1], ret, arg_tys, &env);
    }
    // Strict functions return NULL if any argument is NULL.
    let strict =
        super::sigs::all_sigs().iter().find(|s| s.name == name).map(|s| s.strict).unwrap_or(true);
    if strict && vals.iter().any(Value::is_null) && !matches!(name, "subscript" | "slice") {
        return Ok(Value::Null);
    }
    if let Some(v) = funcs::call(name, &vals, arg_tys, ret, &env)? {
        return Ok(v);
    }
    system_call(name, &vals, arg_tys, ret, ctx)
}

/// Functions that need the catalog, the session or sequences.
fn system_call(name: &str, a: &[Value], tys: &[Type], ret: Type, ctx: &mut Ctx) -> PgResult<Value> {
    let (fmt, now, stmt_now) = env!(ctx);
    let env = Env { fmt: &fmt, now, stmt_now };
    let text = |v: &Value| v.as_str().unwrap_or("").to_string();
    Ok(match name {
        "version" => Value::text(format!(
            "PostgreSQL {} on x86_64-pc-linux-gnu, compiled by noida, 64-bit",
            super::session::SERVER_VERSION
        )),
        "current_database" => Value::text(ctx.rt.database.clone()),
        "current_schema" => match first_schema(ctx) {
            Some(s) => Value::text(s),
            None => Value::Null,
        },
        "current_schemas" => {
            let include_implicit = matches!(a[0], Value::Bool(true));
            let mut out = vec![];
            if include_implicit {
                out.push(Value::text("pg_catalog"));
            }
            for s in ctx.rt.settings.search_path(&ctx.rt.user.clone()) {
                if ctx.db.schema_by_name(&s).is_some() {
                    out.push(Value::text(s));
                }
            }
            Value::Array(Box::new(Array::new(out)))
        }
        "pg_backend_pid" => Value::Int(ctx.rt.pid as i64),
        "pg_typeof" => Value::Int(tys[0].oid() as i64),
        "format_type" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            let typmod = a.get(1).and_then(Value::as_int).map(|v| v as i32).unwrap_or(-1);
            match Type::from_oid(oid) {
                Some(t) => Value::text(t.display(typmod)),
                None => match ctx.db.enums.get(&oid) {
                    Some(e) => Value::text(e.name.clone()),
                    None => Value::text(format!("???({oid})")),
                },
            }
        }
        "current_setting" => {
            let n = text(&a[0]);
            match ctx.rt.settings.get(&n) {
                Ok(v) => Value::text(v),
                Err(e) => {
                    if matches!(a.get(1), Some(Value::Bool(true))) {
                        Value::Null
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        "set_config" => {
            let n = text(&a[0]);
            let v = text(&a[1]);
            ctx.rt.settings.set(&n, &v)?;
            Value::text(v)
        }
        "pg_get_expr" => a[0].clone(),
        "pg_table_is_visible" | "pg_type_is_visible" | "pg_function_is_visible" => {
            Value::Bool(true)
        }
        "pg_get_userbyid" => Value::text(ctx.rt.user.clone()),
        "pg_encoding_to_char" => Value::text("UTF8"),
        "pg_char_to_encoding" => Value::Int(6),
        "pg_client_encoding" => Value::text("UTF8"),
        "pg_is_in_recovery" => Value::Bool(false),
        "pg_jit_available" => Value::Bool(false),
        "pg_trigger_depth" => Value::Int(0),
        "txid_current" | "pg_current_xact_id" => Value::Int(1000),
        "txid_current_if_assigned" => Value::Null,
        "pg_postmaster_start_time" | "pg_conf_load_time" => Value::Ts(ctx.rt.now),
        "inet_server_addr" | "inet_client_addr" => Value::text("127.0.0.1"),
        "inet_server_port" => Value::Int(5432),
        "inet_client_port" => Value::Int(0),
        "pg_relation_size" | "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            let n = ctx.db.table(oid).map_or(0, |t| t.rows.len());
            Value::Int((n as i64) * 64)
        }
        "pg_database_size" => Value::Int(8_388_608),
        "pg_relation_filenode" => a[0].clone(),
        "pg_relation_is_publishable" => Value::Bool(true),
        "pg_sleep" => {
            let secs = funcs::as_f64(&a[0]).clamp(0.0, 10.0);
            std::thread::sleep(std::time::Duration::from_secs_f64(secs));
            Value::Null
        }
        "pg_advisory_lock"
        | "pg_advisory_xact_lock"
        | "pg_advisory_lock_shared"
        | "pg_advisory_unlock_all" => Value::Null,
        "pg_advisory_unlock"
        | "pg_try_advisory_lock"
        | "pg_try_advisory_xact_lock"
        | "pg_advisory_unlock_shared" => Value::Bool(true),
        "pg_cancel_backend" | "pg_terminate_backend" | "pg_reload_conf" => Value::Bool(true),
        "pg_notify" => {
            ctx.notifies.push((text(&a[0]), text(&a[1])));
            Value::Null
        }
        "pg_collation_for" => Value::text("\"default\""),
        "pg_column_size" => Value::Int(match &a[0] {
            Value::Text(s) => s.len() as i64 + 1,
            Value::Int(_) => 8,
            _ => 8,
        }),
        "pg_input_is_valid" => {
            let s = text(&a[0]);
            let tname = text(&a[1]);
            let ty = super::types::TYPES
                .iter()
                .find(|t| t.name == tname || t.display == tname)
                .map(|t| Type::of(t.base));
            match ty {
                Some(t) => Value::Bool(types::from_text(&s, t, &env.dctx()).is_ok()),
                None => Value::Bool(false),
            }
        }
        "has_table_privilege"
        | "has_schema_privilege"
        | "has_database_privilege"
        | "has_column_privilege"
        | "has_sequence_privilege"
        | "has_function_privilege"
        | "has_any_column_privilege"
        | "pg_has_role" => Value::Bool(true),
        "obj_description" | "col_description" | "shobj_description" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            match name {
                "col_description" => {
                    let attnum = a[1].as_int().unwrap_or(0) as usize;
                    ctx.db
                        .table(oid)
                        .and_then(|t| t.columns.get(attnum.saturating_sub(1)))
                        .and_then(|c| c.comment.clone())
                        .map_or(Value::Null, Value::text)
                }
                _ => description_of(ctx, oid).map_or(Value::Null, Value::text),
            }
        }
        "pg_get_constraintdef" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            pgcatalog::constraint_def(ctx.db, oid).map_or(Value::Null, Value::text)
        }
        "pg_get_indexdef" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            pgcatalog::index_def(ctx.db, oid).map_or(Value::Null, Value::text)
        }
        "pg_get_viewdef" => {
            let oid = match &a[0] {
                Value::Int(i) => *i as u32,
                Value::Text(s) => ctx
                    .db
                    .tables
                    .values()
                    .find(|t| {
                        t.name == *s || format!("{}.{}", ctx.db.schema_name(t.schema), t.name) == *s
                    })
                    .map_or(0, |t| t.oid),
                _ => 0,
            };
            ctx.db
                .table(oid)
                .and_then(|t| t.view_sql.clone())
                .map(|s| format!("{s};"))
                .map_or(Value::Null, Value::text)
        }
        "pg_get_serial_sequence" => {
            let tname = text(&a[0]);
            let col = text(&a[1]);
            let tbl = tname.rsplit('.').next().unwrap_or(&tname).trim_matches('"').to_string();
            let found = ctx.db.tables.values().find(|t| t.name == tbl).and_then(|t| {
                let idx = t.col_index(&col)?;
                ctx.db
                    .sequences
                    .values()
                    .find(|s| s.owned_by == Some((t.oid, idx)))
                    .map(|s| format!("{}.{}", ctx.db.schema_name(s.schema), s.name))
            });
            found.map_or(Value::Null, Value::text)
        }
        "pg_get_functiondef"
        | "pg_get_function_arguments"
        | "pg_get_function_result"
        | "pg_get_function_identity_arguments"
        | "pg_get_triggerdef"
        | "pg_get_partkeydef"
        | "pg_get_ruledef"
        | "pg_get_statisticsobjdef_columns"
        | "pg_tablespace_location" => Value::Null,
        "to_regclass" | "to_regtype" | "to_regproc" | "to_regnamespace" | "to_regrole" => {
            let n = text(&a[0]);
            let base = match name {
                "to_regtype" => Base::Regtype,
                "to_regproc" => Base::Regproc,
                "to_regnamespace" => Base::Regnamespace,
                "to_regrole" => Base::Regrole,
                _ => Base::Regclass,
            };
            let path = ctx.rt.settings.search_path(&ctx.rt.user);
            pgcatalog::resolve_reg(ctx.db, &path, &ctx.rt.user, base, &n)
                .map_or(Value::Null, Value::Int)
        }
        "nextval" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            return nextval(ctx, oid).map(Value::Int);
        }
        "currval" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            match ctx.rt.currval.get(&oid) {
                Some(v) => Value::Int(*v),
                None => {
                    let name =
                        ctx.db.sequences.get(&oid).map(|s| s.name.clone()).unwrap_or_default();
                    return Err(PgError::new(
                        code::OBJECT_NOT_IN_PREREQUISITE_STATE,
                        format!(
                            "currval of sequence \"{name}\" is not yet defined in this session"
                        ),
                    ));
                }
            }
        }
        "lastval" => match ctx.rt.lastval.and_then(|o| ctx.rt.currval.get(&o).copied()) {
            Some(v) => Value::Int(v),
            None => {
                return Err(PgError::new(
                    code::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "lastval is not yet defined in this session",
                ));
            }
        },
        "setval" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            let v = a[1].as_int().unwrap_or(0);
            let called = a.get(2).and_then(Value::as_bool).unwrap_or(true);
            ctx.seqs.insert(oid, SeqValue { last: v, is_called: called });
            ctx.rt.currval.insert(oid, v);
            ctx.rt.lastval = Some(oid);
            Value::Int(v)
        }
        "subscript" => {
            let idx = a[1].as_int();
            match (&a[0], idx) {
                (Value::Array(arr), Some(i)) => {
                    if arr.dims.len() > 1 {
                        return Err(PgError::new(
                            code::FEATURE_NOT_SUPPORTED,
                            "slicing multidimensional arrays is not supported",
                        ));
                    }
                    let lb = arr.dims.first().map_or(1, |d| d.1) as i64;
                    let pos = i - lb;
                    if pos < 0 {
                        Value::Null
                    } else {
                        arr.items.get(pos as usize).cloned().unwrap_or(Value::Null)
                    }
                }
                (Value::Jsonb(j), Some(i)) => {
                    j.index(i).cloned().map(|x| Value::Jsonb(Box::new(x))).unwrap_or(Value::Null)
                }
                _ => Value::Null,
            }
        }
        "slice" => match &a[0] {
            Value::Array(arr) => {
                let lb = arr.dims.first().map_or(1, |d| d.1) as i64;
                let len = arr.items.len() as i64;
                let lo = a[1].as_int().unwrap_or(lb).max(lb);
                let hi = a[2].as_int().unwrap_or(lb + len - 1).min(lb + len - 1);
                if lo > hi {
                    Value::Array(Box::new(Array::empty()))
                } else {
                    let items: Vec<Value> =
                        arr.items[(lo - lb) as usize..=(hi - lb) as usize].to_vec();
                    Value::Array(Box::new(Array { dims: vec![(items.len() as i32, 1)], items }))
                }
            }
            _ => Value::Null,
        },
        "like_escape" | "like_escape_ci" => {
            let esc = text(&a[2]).chars().next();
            Value::Bool(funcs::like(&text(&a[0]), &text(&a[1]), name.ends_with("ci"), esc)?)
        }
        "similar_to" => {
            let re = funcs::similar_to_regex(&text(&a[1]), text(&a[2]).chars().next())?;
            let rx = regex_lite::Regex::new(&re).map_err(|e| {
                PgError::new(
                    code::INVALID_REGULAR_EXPRESSION,
                    format!("invalid regular expression: {e}"),
                )
            })?;
            Value::Bool(rx.is_match(&text(&a[0])))
        }
        "current_date" => {
            Value::Date(casts::ts_to_date(super::datetime::utc_to_local(ctx.rt.now, &fmt.zone)))
        }
        "localtimestamp" => Value::Ts(super::datetime::utc_to_local(ctx.rt.now, &fmt.zone)),
        "localtime" => Value::Time(
            super::datetime::utc_to_local(ctx.rt.now, &fmt.zone)
                .rem_euclid(super::datetime::USECS_PER_DAY),
        ),
        "current_time" => {
            let local = super::datetime::utc_to_local(ctx.rt.now, &fmt.zone);
            let off = fmt
                .zone
                .offset_at_utc(ctx.rt.now / 1_000_000 + super::datetime::PG_EPOCH_DAYS * 86400);
            Value::TimeTz(local.rem_euclid(super::datetime::USECS_PER_DAY), off)
        }
        _ => {
            let _ = ret;
            return Err(PgError::new(
                code::FEATURE_NOT_SUPPORTED,
                format!("function {name}() is not implemented"),
            ));
        }
    })
}

fn description_of(ctx: &Ctx, oid: u32) -> Option<String> {
    if let Some(t) = ctx.db.table(oid) {
        return t.comment.clone();
    }
    if let Some(s) = ctx.db.schemas.get(&oid) {
        return s.comment.clone();
    }
    if let Some(s) = ctx.db.sequences.get(&oid) {
        return s.comment.clone();
    }
    ctx.db
        .tables
        .values()
        .find_map(|t| t.constraints.iter().find(|c| c.oid == oid).and_then(|c| c.comment.clone()))
}

fn first_schema(ctx: &mut Ctx) -> Option<String> {
    let user = ctx.rt.user.clone();
    ctx.rt.settings.search_path(&user).into_iter().find(|s| ctx.db.schema_by_name(s).is_some())
}

pub fn nextval(ctx: &mut Ctx, oid: u32) -> PgResult<i64> {
    let seq = ctx
        .db
        .sequences
        .get(&oid)
        .ok_or_else(|| {
            PgError::new(code::UNDEFINED_TABLE, format!("relation with OID {oid} does not exist"))
        })?
        .clone();
    let cur = ctx.seqs.entry(oid).or_insert(SeqValue { last: seq.start, is_called: false });
    let next = if !cur.is_called {
        cur.is_called = true;
        cur.last
    } else {
        let n = cur.last.checked_add(seq.increment).unwrap_or(if seq.increment > 0 {
            seq.max
        } else {
            seq.min
        });
        let wrapped = if seq.increment > 0 && n > seq.max {
            if !seq.cycle {
                return Err(PgError::new(
                    code::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!(
                        "nextval: reached maximum value of sequence \"{}\" ({})",
                        seq.name, seq.max
                    ),
                ));
            }
            seq.min
        } else if seq.increment < 0 && n < seq.min {
            if !seq.cycle {
                return Err(PgError::new(
                    code::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!(
                        "nextval: reached minimum value of sequence \"{}\" ({})",
                        seq.name, seq.min
                    ),
                ));
            }
            seq.max
        } else {
            n
        };
        cur.last = wrapped;
        wrapped
    };
    ctx.rt.currval.insert(oid, next);
    ctx.rt.lastval = Some(oid);
    Ok(next)
}

// ---------------------------------------------------------------------------
// Query execution

pub fn run_query(q: &Query, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    match q {
        Query::Select(s) => run_select(s, ctx),
        Query::Values { rows, order, limit, offset } => {
            let mut out = vec![];
            for r in rows {
                let mut vals = vec![];
                for e in r {
                    vals.push(eval(e, &[], ctx)?);
                }
                out.push(vals);
            }
            sort_rows(&mut out, order);
            apply_limit(&mut out, limit, offset, ctx)?;
            Ok(out)
        }
        Query::SetOp { op, all, left, right, order, limit, offset } => {
            let l = run_query(left, ctx)?;
            let r = run_query(right, ctx)?;
            let mut out = match op {
                SetOpKind::Union => {
                    let mut o = l;
                    o.extend(r);
                    if !all {
                        dedup_rows(&mut o);
                    }
                    o
                }
                SetOpKind::Intersect => {
                    let mut o = vec![];
                    let mut used = vec![false; r.len()];
                    for row in l {
                        if let Some(j) =
                            r.iter().enumerate().position(|(j, x)| !used[j] && rows_equal(x, &row))
                        {
                            if *all {
                                used[j] = true;
                            }
                            o.push(row);
                        }
                    }
                    if !all {
                        dedup_rows(&mut o);
                    }
                    o
                }
                SetOpKind::Except => {
                    let mut o = vec![];
                    let mut used = vec![false; r.len()];
                    for row in l {
                        match r
                            .iter()
                            .enumerate()
                            .position(|(j, x)| !used[j] && rows_equal(x, &row))
                        {
                            Some(j) => {
                                if *all {
                                    used[j] = true;
                                }
                            }
                            None => o.push(row),
                        }
                    }
                    if !all {
                        dedup_rows(&mut o);
                    }
                    o
                }
            };
            sort_rows(&mut out, order);
            apply_limit(&mut out, limit, offset, ctx)?;
            Ok(out)
        }
        Query::With { ctes, body } => {
            for c in ctes {
                let rows = run_query(&c.query, ctx)?;
                if ctx.ctes.len() <= c.slot {
                    ctx.ctes.resize(c.slot + 1, None);
                }
                ctx.ctes[c.slot] = Some(rows);
            }
            run_query(body, ctx)
        }
        Query::Recursive { slot, seed, step, all } => {
            if ctx.ctes.len() <= *slot {
                ctx.ctes.resize(*slot + 1, None);
            }
            let mut result = run_query(seed, ctx)?;
            if !all {
                dedup_rows(&mut result);
            }
            let mut working = result.clone();
            let mut guard = 0;
            while !working.is_empty() {
                guard += 1;
                if guard > 100_000 {
                    return Err(PgError::new(
                        code::PROGRAM_LIMIT_EXCEEDED,
                        "recursive query did not terminate",
                    ));
                }
                ctx.ctes[*slot] = Some(working.clone());
                let mut next = run_query(step, ctx)?;
                if !all {
                    next.retain(|r| !result.iter().any(|x| rows_equal(x, r)));
                    dedup_rows(&mut next);
                }
                result.extend(next.clone());
                working = next;
            }
            ctx.ctes[*slot] = Some(result.clone());
            Ok(result)
        }
        Query::Dml(d) => super::dml::run_dml(d, ctx),
    }
}

fn rows_equal(a: &Row, b: &Row) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| types::cmp_values(x, y) == Ordering::Equal)
}

fn dedup_rows(rows: &mut Vec<Row>) {
    let mut out: Vec<Row> = Vec::with_capacity(rows.len());
    for r in rows.drain(..) {
        if !out.iter().any(|x| rows_equal(x, &r)) {
            out.push(r);
        }
    }
    *rows = out;
}

fn apply_limit(
    rows: &mut Vec<Row>,
    limit: &Option<Expr>,
    offset: &Option<Expr>,
    ctx: &mut Ctx,
) -> PgResult<()> {
    if let Some(o) = offset {
        let v = eval(o, &[], ctx)?;
        if let Some(n) = v.as_int() {
            if n < 0 {
                return Err(PgError::new(
                    code::INVALID_ROW_COUNT_IN_OFFSET,
                    "OFFSET must not be negative",
                ));
            }
            rows.drain(..(n as usize).min(rows.len()));
        }
    }
    if let Some(l) = limit {
        let v = eval(l, &[], ctx)?;
        if let Some(n) = v.as_int() {
            if n < 0 {
                return Err(PgError::new(
                    code::INVALID_ROW_COUNT_IN_LIMIT,
                    "LIMIT must not be negative",
                ));
            }
            rows.truncate(n as usize);
        }
    }
    Ok(())
}

fn sort_rows(rows: &mut [Row], keys: &[SortKey]) {
    if keys.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for k in keys {
            let (x, y) = (&a[k.col], &b[k.col]);
            let o = match (x.is_null(), y.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if k.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if k.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => {
                    let c = types::cmp_values(x, y);
                    if k.desc { c.reverse() } else { c }
                }
            };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
}

fn run_select(s: &Select, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    let mut rows = exec_from(&s.from, ctx)?;
    if let Some(f) = &s.filter {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if truthy(&eval(f, &r, ctx)?) {
                kept.push(r);
            }
        }
        rows = kept;
    }
    // Grouping and aggregation.
    if let Some(keys) = &s.group {
        rows = aggregate(&rows, keys, &s.aggs, ctx)?;
    } else if !s.aggs.is_empty() {
        rows = aggregate(&rows, &[], &s.aggs, ctx)?;
    }
    if let Some(h) = &s.having {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if truthy(&eval(h, &r, ctx)?) {
                kept.push(r);
            }
        }
        rows = kept;
    }
    if !s.windows.is_empty() {
        rows = windows(&rows, &s.windows, ctx)?;
    }
    // Projection (set-returning functions expand into several rows).
    let mut out = if s.srf {
        expand_srfs(&s.proj, &rows, ctx)?
    } else {
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let mut vals = Vec::with_capacity(s.proj.len());
            for e in &s.proj {
                vals.push(eval(e, r, ctx)?);
            }
            out.push(vals);
        }
        out
    };
    match &s.distinct {
        Distinct::None => {}
        Distinct::All => dedup_rows(&mut out),
        Distinct::On(keys) => {
            sort_rows(&mut out, &s.order);
            let mut seen: Vec<Vec<Value>> = vec![];
            out.retain(|r| {
                let k: Vec<Value> = keys.iter().map(|&i| r[i].clone()).collect();
                if seen.iter().any(|x| rows_equal(x, &k)) {
                    false
                } else {
                    seen.push(k);
                    true
                }
            });
        }
    }
    sort_rows(&mut out, &s.order);
    apply_limit(&mut out, &s.limit, &s.offset, ctx)?;
    if s.visible < s.proj.len() {
        for r in out.iter_mut() {
            r.truncate(s.visible);
        }
    }
    Ok(out)
}

/// Expands set-returning functions in the select list.
fn expand_srfs(proj: &[Expr], rows: &[Row], ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    let mut out = vec![];
    for r in rows {
        // Each projection item may produce several values.
        let mut columns: Vec<Vec<Value>> = vec![];
        for e in proj {
            columns.push(eval_multi(e, r, ctx)?);
        }
        let n = columns.iter().map(|c| c.len()).max().unwrap_or(1);
        for i in 0..n {
            let mut row = vec![];
            for c in &columns {
                row.push(if c.len() == 1 {
                    c[0].clone()
                } else {
                    c.get(i).cloned().unwrap_or(Value::Null)
                });
            }
            out.push(row);
        }
    }
    Ok(out)
}

/// Evaluates an expression that may be a set-returning call.
fn eval_multi(e: &Expr, row: &[Value], ctx: &mut Ctx) -> PgResult<Vec<Value>> {
    if let Expr::Call { name, args, arg_tys, ty } = e
        && super::sigs::kind_of(name) == Some(super::sigs::Kind::Srf)
    {
        let mut vals = vec![];
        for a in args {
            vals.push(eval(a, row, ctx)?);
        }
        let rows = srf_rows(name, &vals, arg_tys, *ty, ctx)?;
        return Ok(rows
            .into_iter()
            .map(|mut r| if r.len() == 1 { r.remove(0) } else { Value::Record(r) })
            .collect());
    }
    Ok(vec![eval(e, row, ctx)?])
}

fn exec_from(f: &From, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    Ok(match f {
        From::One => vec![vec![]],
        From::Table { oid, ncols } => {
            let t = ctx.db.table(*oid).ok_or_else(|| {
                PgError::new(
                    code::UNDEFINED_TABLE,
                    format!("relation with OID {oid} does not exist"),
                )
            })?;
            let _ = ncols;
            t.rows.clone()
        }
        From::Virtual { name, .. } => pgcatalog::rows(name, ctx)?,
        From::Cte(slot) => ctx.ctes.get(*slot).cloned().flatten().unwrap_or_default(),
        From::Sub(q) => {
            ctx.outer.push(vec![]);
            let r = run_query(q, ctx);
            ctx.outer.pop();
            r?
        }
        From::Func { name, args, arg_tys, ncols, ordinality, .. } => {
            let mut vals = vec![];
            for a in args {
                vals.push(eval(a, &[], ctx)?);
            }
            let mut rows = srf_rows(name, &vals, arg_tys, Type::TEXT, ctx)?;
            if *ordinality {
                for (i, r) in rows.iter_mut().enumerate() {
                    r.push(Value::Int(i as i64 + 1));
                }
            }
            let _ = ncols;
            rows
        }
        From::Join { left, right, kind, on, lateral, left_cols, right_cols } => {
            let lrows = exec_from(left, ctx)?;
            let mut out = vec![];
            if *lateral {
                for l in &lrows {
                    ctx.outer.push(l.clone());
                    let rrows = exec_from(right, ctx);
                    ctx.outer.pop();
                    let rrows = rrows?;
                    let mut matched = false;
                    for r in rrows {
                        let mut row = l.clone();
                        row.extend(r);
                        if join_ok(on, &row, ctx)? {
                            matched = true;
                            out.push(row);
                        }
                    }
                    if !matched && matches!(kind, JoinKind::Left) {
                        let mut row = l.clone();
                        row.extend(std::iter::repeat_n(Value::Null, *right_cols));
                        out.push(row);
                    }
                }
                return Ok(out);
            }
            let rrows = exec_from(right, ctx)?;
            let mut right_matched = vec![false; rrows.len()];
            for l in &lrows {
                let mut matched = false;
                for (j, r) in rrows.iter().enumerate() {
                    let mut row = l.clone();
                    row.extend(r.clone());
                    if join_ok(on, &row, ctx)? {
                        matched = true;
                        right_matched[j] = true;
                        out.push(row);
                    }
                }
                if !matched && matches!(kind, JoinKind::Left | JoinKind::Full) {
                    let mut row = l.clone();
                    row.extend(std::iter::repeat_n(Value::Null, *right_cols));
                    out.push(row);
                }
            }
            if matches!(kind, JoinKind::Right | JoinKind::Full) {
                for (j, r) in rrows.iter().enumerate() {
                    if !right_matched[j] {
                        let mut row = vec![Value::Null; *left_cols];
                        row.extend(r.clone());
                        out.push(row);
                    }
                }
            }
            out
        }
    })
}

fn join_ok(on: &Option<Expr>, row: &[Value], ctx: &mut Ctx) -> PgResult<bool> {
    match on {
        None => Ok(true),
        Some(e) => Ok(truthy(&eval(e, row, ctx)?)),
    }
}

// ---------------------------------------------------------------------------
// Set-returning functions

fn srf_rows(
    name: &str,
    a: &[Value],
    tys: &[Type],
    _ret: Type,
    ctx: &mut Ctx,
) -> PgResult<Vec<Row>> {
    let (fmt, now, stmt_now) = env!(ctx);
    let env = Env { fmt: &fmt, now, stmt_now };
    let one = |v: Vec<Value>| -> Vec<Row> { v.into_iter().map(|x| vec![x]).collect() };
    Ok(match name {
        "generate_series" => {
            if a.iter().any(Value::is_null) {
                return Ok(vec![]);
            }
            match (&a[0], &a[1]) {
                (Value::Int(from), Value::Int(to)) => {
                    let step = a.get(2).and_then(Value::as_int).unwrap_or(1);
                    if step == 0 {
                        return Err(PgError::new(
                            code::INVALID_PARAMETER_VALUE,
                            "step size cannot equal zero",
                        ));
                    }
                    let mut out = vec![];
                    let mut v = *from;
                    while (step > 0 && v <= *to) || (step < 0 && v >= *to) {
                        out.push(vec![Value::Int(v)]);
                        match v.checked_add(step) {
                            Some(n) => v = n,
                            None => break,
                        }
                    }
                    out
                }
                (Value::Num(from), Value::Num(to)) => {
                    let step = match a.get(2) {
                        Some(Value::Num(s)) => s.clone(),
                        _ => Numeric::from_i64(1),
                    };
                    if step.is_zero() {
                        return Err(PgError::new(
                            code::INVALID_PARAMETER_VALUE,
                            "step size cannot equal zero",
                        ));
                    }
                    let mut out = vec![];
                    let mut v = from.clone();
                    let up = !step.is_negative();
                    loop {
                        let c = super::numeric::cmp_num(&v, to);
                        if (up && c == Ordering::Greater) || (!up && c == Ordering::Less) {
                            break;
                        }
                        out.push(vec![Value::Num(v.clone())]);
                        v = v.add(&step);
                        if out.len() > 1_000_000 {
                            break;
                        }
                    }
                    out
                }
                (Value::Ts(from), Value::Ts(to)) => {
                    let Some(Value::Interval(step)) = a.get(2) else {
                        return Err(PgError::new(
                            code::INVALID_PARAMETER_VALUE,
                            "step size cannot equal zero",
                        ));
                    };
                    if step.months == 0 && step.days == 0 && step.micros == 0 {
                        return Err(PgError::new(
                            code::INVALID_PARAMETER_VALUE,
                            "step size cannot equal zero",
                        ));
                    }
                    let up = step.span() > 0;
                    let tz = tys[0].base == Base::Timestamptz;
                    let mut out = vec![];
                    let mut v = *from;
                    loop {
                        if (up && v > *to) || (!up && v < *to) {
                            break;
                        }
                        out.push(vec![Value::Ts(v)]);
                        let next = if tz {
                            super::datetime::timestamptz_add(v, step, &fmt.zone)
                        } else {
                            super::datetime::timestamp_add(v, step)
                        };
                        match next {
                            Ok(n) if n != v => v = n,
                            _ => break,
                        }
                        if out.len() > 1_000_000 {
                            break;
                        }
                    }
                    out
                }
                _ => vec![],
            }
        }
        "generate_subscripts" => {
            let dim = a[1].as_int().unwrap_or(1) as usize;
            match &a[0] {
                Value::Array(arr) if dim >= 1 && dim <= arr.dims.len() => {
                    let (len, lb) = arr.dims[dim - 1];
                    (0..len).map(|i| vec![Value::Int((lb + i) as i64)]).collect()
                }
                _ => vec![],
            }
        }
        "unnest" => match &a[0] {
            Value::Array(arr) => one(arr.items.clone()),
            Value::Null => vec![],
            other => one(vec![other.clone()]),
        },
        "jsonb_array_elements"
        | "json_array_elements"
        | "jsonb_array_elements_text"
        | "json_array_elements_text" => {
            let j = json_arg(&a[0])?;
            let jsonb = name.starts_with("jsonb");
            let text = name.ends_with("_text");
            let Json::Array(items) = j else {
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    "cannot extract elements from an object",
                ));
            };
            items
                .into_iter()
                .map(|x| {
                    vec![if text {
                        x.as_text(jsonb).map_or(Value::Null, Value::Text)
                    } else if jsonb {
                        Value::Jsonb(Box::new(x))
                    } else {
                        Value::text(x.to_compact_string())
                    }]
                })
                .collect()
        }
        "jsonb_each" | "json_each" | "jsonb_each_text" | "json_each_text" => {
            let j = json_arg(&a[0])?;
            let jsonb = name.starts_with("jsonb");
            let text = name.ends_with("_text");
            let Json::Object(members) = j else {
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    "cannot call jsonb_each on a non-object",
                ));
            };
            members
                .into_iter()
                .map(|(k, v)| {
                    vec![
                        Value::Text(k),
                        if text {
                            v.as_text(jsonb).map_or(Value::Null, Value::Text)
                        } else if jsonb {
                            Value::Jsonb(Box::new(v))
                        } else {
                            Value::text(v.to_compact_string())
                        },
                    ]
                })
                .collect()
        }
        "jsonb_object_keys" | "json_object_keys" => {
            let j = json_arg(&a[0])?;
            let Json::Object(members) = j else {
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    "cannot call jsonb_object_keys on a non-object",
                ));
            };
            members.into_iter().map(|(k, _)| vec![Value::Text(k)]).collect()
        }
        "regexp_split_to_table" => {
            let parts = funcs::regex_split_pub(
                a[1].as_str().unwrap_or(""),
                a.get(2).and_then(Value::as_str).unwrap_or(""),
                a[0].as_str().unwrap_or(""),
            )?;
            parts.into_iter().map(|p| vec![Value::Text(p)]).collect()
        }
        "regexp_matches" => {
            let flags = a.get(2).and_then(Value::as_str).unwrap_or("");
            let re = funcs::regex(a[1].as_str().unwrap_or(""), flags)?;
            let hay = a[0].as_str().unwrap_or("");
            let mut out = vec![];
            for c in re.captures_iter(hay) {
                let items: Vec<Value> = if c.len() == 1 {
                    vec![c.get(0).map_or(Value::Null, |m| Value::text(m.as_str()))]
                } else {
                    (1..c.len())
                        .map(|i| c.get(i).map_or(Value::Null, |m| Value::text(m.as_str())))
                        .collect()
                };
                out.push(vec![Value::Array(Box::new(Array::new(items)))]);
                if !flags.contains('g') {
                    break;
                }
            }
            out
        }
        "pg_listening_channels" => {
            ctx.rt.listening.iter().map(|c| vec![Value::text(c.clone())]).collect()
        }
        "pg_get_keywords" => super::keywords::RESERVED
            .split_ascii_whitespace()
            .map(|w| {
                vec![
                    Value::text(w),
                    Value::text("R"),
                    Value::Bool(false),
                    Value::text("reserved"),
                    Value::text("can not be bare label"),
                ]
            })
            .collect(),
        "jsonb_path_query" => {
            return Err(PgError::new(code::FEATURE_NOT_SUPPORTED, "jsonpath is not supported"));
        }
        _ => {
            let _ = &env;
            return Err(PgError::new(
                code::FEATURE_NOT_SUPPORTED,
                format!("set-returning function {name}() is not implemented"),
            ));
        }
    })
}

fn json_arg(v: &Value) -> PgResult<Json> {
    Ok(match v {
        Value::Jsonb(j) => (**j).clone(),
        Value::Text(s) => {
            super::json::parse(s).map_err(|e| types::invalid_input("json", s).detail(e.0))?
        }
        _ => Json::Null,
    })
}

// ---------------------------------------------------------------------------
// Aggregation

#[derive(Debug)]
enum AggState {
    Count(i64),
    SumInt(Option<i128>),
    SumNum(Option<Numeric>),
    SumFloat(Option<f64>),
    SumInterval(Option<super::datetime::Interval>),
    MinMax(Option<Value>, bool),
    Bool(Option<bool>, bool),
    BitOp(Option<i64>, bool),
    Values(Vec<Value>),
    Stats(Vec<f64>),
}

fn new_state(agg: &AggCall) -> AggState {
    let arg = agg.arg_tys.first().copied().unwrap_or(Type::TEXT);
    match agg.name {
        "count" => AggState::Count(0),
        "sum" | "avg" => match arg.base {
            // sum(int8) and every avg() accumulate in numeric.
            Base::Int2 | Base::Int4 if agg.name != "avg" => AggState::SumInt(None),
            Base::Int2 | Base::Int4 | Base::Int8 => AggState::SumNum(None),
            Base::Numeric => AggState::SumNum(None),
            Base::Interval => AggState::SumInterval(None),
            _ => AggState::SumFloat(None),
        },
        "min" => AggState::MinMax(None, false),
        "max" => AggState::MinMax(None, true),
        "bool_and" | "every" => AggState::Bool(None, true),
        "bool_or" => AggState::Bool(None, false),
        "bit_and" => AggState::BitOp(None, true),
        "bit_or" => AggState::BitOp(None, false),
        "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop" => {
            AggState::Stats(vec![])
        }
        _ => AggState::Values(vec![]),
    }
}

fn accumulate(st: &mut AggState, agg: &AggCall, vals: &[Value], n_rows: &mut i64) -> PgResult<()> {
    let v = vals.first().cloned().unwrap_or(Value::Null);
    match st {
        AggState::Count(c) => {
            if agg.star || !v.is_null() {
                *c += 1;
            }
        }
        _ if v.is_null() && !matches!(st, AggState::Values(_)) => {}
        AggState::SumInt(acc) => {
            if let Some(i) = v.as_int() {
                *acc = Some(acc.unwrap_or(0) + i as i128);
                *n_rows += 1;
            }
        }
        AggState::SumNum(acc) => {
            let n = funcs::as_num(&v);
            *acc = Some(match acc {
                Some(a) => a.add(&n),
                None => n,
            });
            *n_rows += 1;
        }
        AggState::SumFloat(acc) => {
            let f = funcs::as_f64(&v);
            *acc = Some(acc.unwrap_or(0.0) + f);
            *n_rows += 1;
        }
        AggState::SumInterval(acc) => {
            if let Value::Interval(iv) = v {
                *acc = Some(match acc {
                    Some(a) => a.add(&iv).map_err(|_| {
                        PgError::new(code::DATETIME_FIELD_OVERFLOW, "interval out of range")
                    })?,
                    None => iv,
                });
                *n_rows += 1;
            }
        }
        AggState::MinMax(best, is_max) => {
            let take = match best {
                None => true,
                Some(b) => {
                    let c = types::cmp_values(&v, b);
                    if *is_max { c == Ordering::Greater } else { c == Ordering::Less }
                }
            };
            if take {
                *best = Some(v);
            }
        }
        AggState::Bool(acc, is_and) => {
            if let Value::Bool(b) = v {
                *acc = Some(match acc {
                    None => b,
                    Some(a) => {
                        if *is_and {
                            *a && b
                        } else {
                            *a || b
                        }
                    }
                });
            }
        }
        AggState::BitOp(acc, is_and) => {
            if let Some(i) = v.as_int() {
                *acc = Some(match acc {
                    None => i,
                    Some(a) => {
                        if *is_and {
                            *a & i
                        } else {
                            *a | i
                        }
                    }
                });
            }
        }
        AggState::Stats(xs) => xs.push(funcs::as_f64(&v)),
        AggState::Values(items) => {
            if vals.len() > 1 {
                items.push(Value::Record(vals.to_vec()));
            } else {
                items.push(v);
            }
        }
    }
    Ok(())
}

fn finish(st: AggState, agg: &AggCall, n_rows: i64, ctx: &mut Ctx) -> PgResult<Value> {
    let (fmt, now, stmt_now) = env!(ctx);
    let env = Env { fmt: &fmt, now, stmt_now };
    Ok(match st {
        AggState::Count(c) => Value::Int(c),
        AggState::SumInt(v) => match v {
            None => Value::Null,
            Some(i) => Value::Int(i64::try_from(i).map_err(|_| {
                PgError::new(code::NUMERIC_VALUE_OUT_OF_RANGE, "bigint out of range")
            })?),
        },
        AggState::SumNum(v) => {
            match v {
                None => Value::Null,
                Some(n) => {
                    if agg.name == "avg" {
                        let d = Numeric::from_i64(n_rows);
                        Value::Num(n.div(&d).map_err(|_| {
                            PgError::new(code::DIVISION_BY_ZERO, "division by zero")
                        })?)
                    } else {
                        Value::Num(n)
                    }
                }
            }
        }
        AggState::SumFloat(v) => match v {
            None => Value::Null,
            Some(f) => {
                if agg.name == "avg" {
                    Value::Float(f / n_rows as f64)
                } else if agg.ty.base == Base::Float4 {
                    Value::Float(f as f32 as f64)
                } else {
                    Value::Float(f)
                }
            }
        },
        AggState::SumInterval(v) => match v {
            None => Value::Null,
            Some(iv) => {
                if agg.name == "avg" {
                    Value::Interval(iv.div(n_rows as f64).map_err(|_| {
                        PgError::new(code::DATETIME_FIELD_OVERFLOW, "interval out of range")
                    })?)
                } else {
                    Value::Interval(iv)
                }
            }
        },
        AggState::MinMax(v, _) => v.unwrap_or(Value::Null),
        AggState::Bool(v, _) => v.map_or(Value::Null, Value::Bool),
        AggState::BitOp(v, _) => v.map_or(Value::Null, Value::Int),
        AggState::Stats(xs) => {
            let n = xs.len() as f64;
            let pop = agg.name.ends_with("_pop");
            if xs.is_empty() || (!pop && xs.len() < 2) {
                return Ok(Value::Null);
            }
            let mean = xs.iter().sum::<f64>() / n;
            let ss: f64 = xs.iter().map(|x| (x - mean) * (x - mean)).sum();
            let var = if pop { ss / n } else { ss / (n - 1.0) };
            let r = if agg.name.starts_with("stddev") { var.sqrt() } else { var };
            if agg.ty.base == Base::Numeric {
                Value::Num(
                    Numeric::parse(&types::format_float(r, false, 1)).unwrap_or(Numeric::NaN),
                )
            } else {
                Value::Float(r)
            }
        }
        AggState::Values(items) => finish_collect(agg, items, &env)?,
    })
}

fn finish_collect(agg: &AggCall, items: Vec<Value>, env: &Env) -> PgResult<Value> {
    let arg = agg.arg_tys.first().copied().unwrap_or(Type::TEXT);
    Ok(match agg.name {
        "array_agg" => {
            if items.is_empty() {
                Value::Null
            } else {
                Value::Array(Box::new(Array::new(items)))
            }
        }
        "string_agg" => {
            let sep = agg.arg_tys.len();
            let _ = sep;
            let mut parts = vec![];
            let mut delim = String::new();
            for v in &items {
                if let Value::Record(r) = v {
                    if r[0].is_null() {
                        continue;
                    }
                    parts.push(funcs::to_str(&r[0], arg, env));
                    delim = r.get(1).map(|d| funcs::to_str(d, Type::TEXT, env)).unwrap_or_default();
                } else if !v.is_null() {
                    parts.push(funcs::to_str(v, arg, env));
                }
            }
            if parts.is_empty() { Value::Null } else { Value::text(parts.join(&delim)) }
        }
        "json_agg" | "jsonb_agg" => {
            let vals: Vec<Json> = items.iter().map(|v| funcs::to_json_value(v, arg, env)).collect();
            if agg.name == "jsonb_agg" {
                Value::Jsonb(Box::new(Json::Array(vals).normalize()))
            } else {
                let parts: Vec<String> =
                    items.iter().map(|v| funcs::to_json_text(v, arg, env)).collect();
                Value::text(format!("[{}]", parts.join(", ")))
            }
        }
        "json_object_agg" | "jsonb_object_agg" => {
            let mut members = vec![];
            for v in &items {
                if let Value::Record(r) = v {
                    if r[0].is_null() {
                        return Err(PgError::new(
                            code::NULL_VALUE_NOT_ALLOWED,
                            "field name must not be null",
                        ));
                    }
                    let k = funcs::to_str(&r[0], agg.arg_tys[0], env);
                    let val = funcs::to_json_value(&r[1], agg.arg_tys[1], env);
                    members.push((k, val));
                }
            }
            if agg.name == "jsonb_object_agg" {
                Value::Jsonb(Box::new(Json::Object(members).normalize()))
            } else {
                let parts: Vec<String> = members
                    .iter()
                    .map(|(k, v)| format!("{} : {}", super::json::escape(k), v.to_compact_string()))
                    .collect();
                Value::text(format!("{{ {} }}", parts.join(", ")))
            }
        }
        "mode" => {
            let mut best: Option<(Value, usize)> = None;
            for v in &items {
                if v.is_null() {
                    continue;
                }
                let c = items.iter().filter(|x| types::cmp_values(x, v) == Ordering::Equal).count();
                if best.as_ref().is_none_or(|(_, bc)| c > *bc) {
                    best = Some((v.clone(), c));
                }
            }
            best.map_or(Value::Null, |(v, _)| v)
        }
        _ => Value::Null,
    })
}

fn aggregate(rows: &[Row], keys: &[Expr], aggs: &[AggCall], ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    struct Group {
        key: Vec<Value>,
        states: Vec<AggState>,
        counts: Vec<i64>,
        seen: Vec<Vec<Value>>,
    }
    let mut groups: Vec<Group> = vec![];
    for r in rows {
        let mut key = Vec::with_capacity(keys.len());
        for k in keys {
            key.push(eval(k, r, ctx)?);
        }
        let idx = match groups.iter().position(|g| rows_equal(&g.key, &key)) {
            Some(i) => i,
            None => {
                groups.push(Group {
                    key,
                    states: aggs.iter().map(new_state).collect(),
                    counts: vec![0; aggs.len()],
                    seen: vec![vec![]; aggs.len()],
                });
                groups.len() - 1
            }
        };
        for (i, agg) in aggs.iter().enumerate() {
            if let Some(f) = &agg.filter
                && !truthy(&eval(f, r, ctx)?)
            {
                continue;
            }
            let mut vals = Vec::with_capacity(agg.args.len());
            for arg in &agg.args {
                vals.push(eval(arg, r, ctx)?);
            }
            if agg.distinct {
                let first = vals.first().cloned().unwrap_or(Value::Null);
                if groups[idx].seen[i]
                    .iter()
                    .any(|x| types::cmp_values(x, &first) == Ordering::Equal)
                {
                    continue;
                }
                groups[idx].seen[i].push(first);
            }
            let g = &mut groups[idx];
            accumulate(&mut g.states[i], agg, &vals, &mut g.counts[i])?;
        }
    }
    // An aggregate with no GROUP BY over no rows still returns one row.
    if groups.is_empty() && keys.is_empty() {
        groups.push(Group {
            key: vec![],
            states: aggs.iter().map(new_state).collect(),
            counts: vec![0; aggs.len()],
            seen: vec![],
        });
    }
    let mut out = vec![];
    for g in groups {
        let mut row = g.key;
        for (i, (st, agg)) in g.states.into_iter().zip(aggs).enumerate() {
            row.push(finish(st, agg, g.counts[i], ctx)?);
        }
        out.push(row);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Window functions

fn windows(rows: &[Row], wins: &[WinCall], ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    let mut out: Vec<Row> = rows.to_vec();
    for (wi, w) in wins.iter().enumerate() {
        let mut results = vec![Value::Null; rows.len()];
        // Partition rows by the PARTITION BY key.
        let mut parts: Vec<(Vec<Value>, Vec<usize>)> = vec![];
        for (i, r) in rows.iter().enumerate() {
            let mut key = vec![];
            for p in &w.partition {
                key.push(eval(p, r, ctx)?);
            }
            match parts.iter_mut().find(|(k, _)| rows_equal(k, &key)) {
                Some((_, idxs)) => idxs.push(i),
                None => parts.push((key, vec![i])),
            }
        }
        for (_, idxs) in &mut parts {
            // Sort the partition by the window ORDER BY.
            if !w.order.is_empty() {
                let mut keyed: Vec<(Vec<Value>, usize)> = vec![];
                for &i in idxs.iter() {
                    let mut k = vec![];
                    for (e, _, _) in &w.order {
                        k.push(eval(e, &rows[i], ctx)?);
                    }
                    keyed.push((k, i));
                }
                let specs: Vec<(bool, bool)> = w.order.iter().map(|(_, d, n)| (*d, *n)).collect();
                keyed.sort_by(|a, b| {
                    for (i, (desc, nulls_first)) in specs.iter().enumerate() {
                        let (x, y) = (&a.0[i], &b.0[i]);
                        let o = match (x.is_null(), y.is_null()) {
                            (true, true) => Ordering::Equal,
                            (true, false) => {
                                if *nulls_first {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                            (false, true) => {
                                if *nulls_first {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                            _ => {
                                let c = types::cmp_values(x, y);
                                if *desc { c.reverse() } else { c }
                            }
                        };
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    Ordering::Equal
                });
                *idxs = keyed.into_iter().map(|(_, i)| i).collect();
            }
            compute_window(w, idxs, rows, &mut results, ctx)?;
        }
        for (i, r) in out.iter_mut().enumerate() {
            let v = std::mem::replace(&mut results[i], Value::Null);
            if r.len() == rows[i].len() + wi {
                r.push(v);
            }
        }
    }
    Ok(out)
}

fn compute_window(
    w: &WinCall,
    idxs: &[usize],
    rows: &[Row],
    results: &mut [Value],
    ctx: &mut Ctx,
) -> PgResult<()> {
    let n = idxs.len();
    // Peer groups for rank/dense_rank/percent_rank.
    let mut order_keys: Vec<Vec<Value>> = vec![];
    for &i in idxs {
        let mut k = vec![];
        for (e, _, _) in &w.order {
            k.push(eval(e, &rows[i], ctx)?);
        }
        order_keys.push(k);
    }
    let same_peer = |a: usize, b: usize| -> bool {
        !order_keys.is_empty() && rows_equal(&order_keys[a], &order_keys[b])
    };
    match w.name {
        "row_number" => {
            for (p, &i) in idxs.iter().enumerate() {
                results[i] = Value::Int(p as i64 + 1);
            }
        }
        "rank" | "dense_rank" | "percent_rank" | "cume_dist" => {
            let mut rank = 1i64;
            let mut dense = 1i64;
            for p in 0..n {
                if p > 0 && !same_peer(p - 1, p) {
                    rank = p as i64 + 1;
                    dense += 1;
                }
                let v = match w.name {
                    "rank" => Value::Int(rank),
                    "dense_rank" => Value::Int(dense),
                    "percent_rank" => {
                        Value::Float(if n > 1 { (rank - 1) as f64 / (n - 1) as f64 } else { 0.0 })
                    }
                    _ => {
                        // cume_dist: rows <= current peer group / total.
                        let mut cnt = p + 1;
                        while cnt < n && same_peer(p, cnt) {
                            cnt += 1;
                        }
                        Value::Float(cnt as f64 / n as f64)
                    }
                };
                results[idxs[p]] = v;
            }
        }
        "ntile" => {
            let buckets = eval(&w.args[0], &rows[idxs[0]], ctx)?.as_int().unwrap_or(1).max(1);
            for p in 0..n {
                let b = (p as i64 * buckets) / n as i64 + 1;
                results[idxs[p]] = Value::Int(b);
            }
        }
        "lag" | "lead" => {
            let offset = match w.args.get(1) {
                Some(e) => eval(e, &rows[idxs[0]], ctx)?.as_int().unwrap_or(1),
                None => 1,
            };
            for p in 0..n {
                let target = if w.name == "lag" { p as i64 - offset } else { p as i64 + offset };
                let v = if target >= 0 && (target as usize) < n {
                    eval(&w.args[0], &rows[idxs[target as usize]], ctx)?
                } else {
                    match w.args.get(2) {
                        Some(d) => eval(d, &rows[idxs[p]], ctx)?,
                        None => Value::Null,
                    }
                };
                results[idxs[p]] = v;
            }
        }
        "first_value" | "last_value" | "nth_value" => {
            for p in 0..n {
                let (start, end) = frame_bounds(w, p, n, ctx, &rows[idxs[p]])?;
                let pick = match w.name {
                    "first_value" => start,
                    "last_value" => end.saturating_sub(1),
                    _ => {
                        let nth = eval(&w.args[1], &rows[idxs[p]], ctx)?.as_int().unwrap_or(1);
                        start + (nth.max(1) as usize - 1)
                    }
                };
                results[idxs[p]] = if pick < end && pick < n {
                    eval(&w.args[0], &rows[idxs[pick]], ctx)?
                } else {
                    Value::Null
                };
            }
        }
        _ => {
            // An aggregate used as a window function.
            let Some(agg) = &w.agg else {
                return Err(PgError::new(
                    code::FEATURE_NOT_SUPPORTED,
                    format!("window function {} is not implemented", w.name),
                ));
            };
            for p in 0..n {
                let (start, mut end) = frame_bounds(w, p, n, ctx, &rows[idxs[p]])?;
                if w.frame.is_none() && !w.order.is_empty() {
                    // Default frame: RANGE UNBOUNDED PRECEDING TO CURRENT ROW (peers included).
                    end = p + 1;
                    while end < n && same_peer(p, end) {
                        end += 1;
                    }
                }
                let mut st = new_state(agg);
                let mut count = 0;
                let mut seen: Vec<Value> = vec![];
                for q in start..end.min(n) {
                    let r = &rows[idxs[q]];
                    if let Some(f) = &agg.filter
                        && !truthy(&eval(f, r, ctx)?)
                    {
                        continue;
                    }
                    let mut vals = vec![];
                    for arg in &agg.args {
                        vals.push(eval(arg, r, ctx)?);
                    }
                    if agg.distinct {
                        let first = vals.first().cloned().unwrap_or(Value::Null);
                        if seen.iter().any(|x| types::cmp_values(x, &first) == Ordering::Equal) {
                            continue;
                        }
                        seen.push(first);
                    }
                    accumulate(&mut st, agg, &vals, &mut count)?;
                }
                results[idxs[p]] = finish(st, agg, count, ctx)?;
            }
        }
    }
    Ok(())
}

fn frame_bounds(
    w: &WinCall,
    p: usize,
    n: usize,
    ctx: &mut Ctx,
    row: &Row,
) -> PgResult<(usize, usize)> {
    let Some(f) = &w.frame else {
        return Ok(if w.order.is_empty() { (0, n) } else { (0, p + 1) });
    };
    let val = |b: &FrameBound, ctx: &mut Ctx| -> PgResult<i64> {
        Ok(match b {
            FrameBound::Preceding(e) | FrameBound::Following(e) => {
                eval(e, row, ctx)?.as_int().unwrap_or(0)
            }
            _ => 0,
        })
    };
    let start = match &f.start {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::CurrentRow => p,
        FrameBound::Preceding(_) => p.saturating_sub(val(&f.start, ctx)? as usize),
        FrameBound::Following(_) => p + val(&f.start, ctx)? as usize,
        FrameBound::UnboundedFollowing => n,
    };
    let end = match &f.end {
        FrameBound::UnboundedFollowing => n,
        FrameBound::CurrentRow => p + 1,
        FrameBound::Following(_) => (p + val(&f.end, ctx)? as usize + 1).min(n),
        FrameBound::Preceding(_) => (p + 1).saturating_sub(val(&f.end, ctx)? as usize),
        FrameBound::UnboundedPreceding => 0,
    };
    Ok((start.min(n), end.min(n)))
}
