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
/// An open cursor, as `pg_cursors` lists it.
#[derive(Clone, Debug)]
pub struct CursorInfo {
    pub name: String,
    pub statement: String,
    pub holdable: bool,
    pub binary: bool,
    pub scrollable: bool,
    pub created: i64,
}

pub struct Runtime {
    /// The session's open cursors when the statement started (`pg_cursors`).
    pub cursors: Vec<CursorInfo>,
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
    /// Set by a CancelRequest for this session.
    pub cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// When the running statement exceeds statement_timeout.
    pub deadline: Option<std::time::Instant>,
    /// Work done since the last interrupt check.
    pub ticks: u32,
    /// `SET CONSTRAINTS ALL DEFERRED|IMMEDIATE` in this transaction.
    pub deferred_all: Option<bool>,
    /// `SET CONSTRAINTS name DEFERRED|IMMEDIATE`, by constraint name.
    pub deferred: BTreeMap<String, bool>,
    /// Settings set LOCAL in this transaction (restored at its end).
    pub local_settings: Vec<String>,
}

impl Runtime {
    /// Starts a statement: an earlier cancel no longer applies, and
    /// statement_timeout starts counting.
    pub fn start_statement(&mut self) {
        self.cancel.store(false, std::sync::atomic::Ordering::SeqCst);
        let ms = self
            .settings
            .get("statement_timeout")
            .ok()
            .and_then(|v| super::session::parse_ms(&v))
            .unwrap_or(0);
        self.deadline = (ms > 0)
            .then(|| std::time::Instant::now() + std::time::Duration::from_millis(ms as u64));
    }

    /// Fails the statement if it was canceled or ran out of time.
    pub fn check_interrupt(&self) -> PgResult<()> {
        if self.cancel.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(PgError::new(
                code::QUERY_CANCELED,
                "canceling statement due to user request",
            ));
        }
        if self.deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Err(PgError::new(
                code::QUERY_CANCELED,
                "canceling statement due to statement timeout",
            ));
        }
        Ok(())
    }

    /// `check_interrupt`, every so many calls (cheap enough for hot loops).
    #[inline]
    pub fn tick(&mut self) -> PgResult<()> {
        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks & 4095 == 0 { self.check_interrupt() } else { Ok(()) }
    }
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
    pub databases: Vec<(u32, String)>,
    /// Results of subqueries that read no outer row, one map per running
    /// query (by the subquery plan's address): like Postgres's InitPlans,
    /// they run once per execution of the query containing them.
    pub subq_cache: Vec<std::collections::HashMap<usize, std::sync::Arc<Vec<Row>>>>,
    /// The outermost `outer` level read since the innermost running
    /// subquery started (how it tells whether it is correlated).
    pub min_outer: usize,
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
    ctx.rt.tick()?;
    Ok(match e {
        Expr::Const(v) => v.clone(),
        Expr::UserFunc { oid, args } => {
            let mut vals = Vec::with_capacity(args.len());
            for a in args {
                vals.push(eval(a, row, ctx)?);
            }
            let func = ctx.db.functions.get(oid).cloned().ok_or_else(|| {
                PgError::new(
                    code::UNDEFINED_FUNCTION,
                    format!("function with OID {oid} does not exist"),
                )
            })?;
            let info = super::dml::session_info(ctx);
            super::plpgsql::call_function(ctx, &info, &func, vals)?
        }
        Expr::Param(i) => ctx.params.get(*i).cloned().unwrap_or(Value::Null),
        Expr::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
        Expr::Outer(depth, i) => {
            let n = ctx.outer.len();
            ctx.min_outer = ctx.min_outer.min(n.wrapping_sub(*depth));
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
                let path = ctx.rt.settings.lookup_path(&ctx.rt.user);
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
            if let (Base::Enum(oid), Value::Text(s)) = (to.base, &v)
                && !to.array
                && let Some(e) = ctx.db.enums.get(&oid)
                && !e.labels.iter().any(|(_, l, _)| l == s)
            {
                return Err(PgError::new(
                    code::INVALID_TEXT_REPRESENTATION,
                    format!(
                        "invalid input value for enum {}: \"{s}\"",
                        funcs::quote_ident(&e.name)
                    ),
                ));
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
            for r in rows.iter() {
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
                    rows.iter().map(|r| r.first().cloned().unwrap_or(Value::Null)).collect(),
                ))),
                SubKind::Scalar => {
                    if rows.len() > 1 {
                        return Err(PgError::new(
                            code::CARDINALITY_VIOLATION,
                            "more than one row returned by a subquery used as an expression",
                        ));
                    }
                    rows.first().and_then(|r| r.first()).cloned().unwrap_or(Value::Null)
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

/// Runs a subquery for `row` of the query containing it. A run that read
/// nothing from `row` (or further out) would give the same rows for any
/// other row, so they're kept for the rest of the containing query.
fn run_subquery(q: &Query, row: &[Value], ctx: &mut Ctx) -> PgResult<std::sync::Arc<Vec<Row>>> {
    let key = q as *const Query as usize;
    if let Some(rows) = ctx.subq_cache.last().and_then(|m| m.get(&key)) {
        return Ok(rows.clone());
    }
    let base = ctx.outer.len();
    let saved = std::mem::replace(&mut ctx.min_outer, usize::MAX);
    ctx.outer.push(row.to_vec());
    let r = run_query(q, ctx);
    ctx.outer.pop();
    let reached = ctx.min_outer;
    ctx.min_outer = saved.min(reached);
    let rows = std::sync::Arc::new(r?);
    if reached > base
        && let Some(m) = ctx.subq_cache.last_mut()
    {
        m.insert(key, rows.clone());
    }
    Ok(rows)
}

/// Names for reg* values, built from the live catalog.
fn reg_names(ctx: &Ctx) -> types::RegNames {
    build_reg_names(ctx.db, &ctx.rt.user, &ctx.rt.settings.lookup_path(&ctx.rt.user))
}

/// Names `reg*` values print as: identifiers quoted the way Postgres does.
pub fn build_reg_names(db: &DbState, user: &str, path: &[String]) -> types::RegNames {
    let mut r = types::RegNames::default();
    // Catalog relations first, so a user object can never be shadowed by
    // one (OIDs don't collide in practice, but user objects should win).
    for (oid, schema, name) in super::pgcatalog::system_relation_names() {
        r.class.insert(oid, db.regclass_text(schema, name, path));
    }
    for t in db.tables.values() {
        r.class.insert(t.oid, db.regclass_text(t.schema, &t.name, path));
        for i in &t.indexes {
            r.class.insert(i.oid, db.regclass_text(t.schema, &i.name, path));
        }
    }
    for s in db.sequences.values() {
        r.class.insert(s.oid, db.regclass_text(s.schema, &s.name, path));
    }
    for ti in types::TYPES {
        r.types.insert(ti.oid, Type::of(ti.base).display(-1));
        if ti.array_oid != 0 {
            r.types.insert(ti.array_oid, Type::array_of(ti.base).display(-1));
        }
    }
    for e in db.enums.values() {
        r.types.insert(e.oid, db.regclass_text(e.schema, &e.name, path));
    }
    for sig in super::sigs::all_sigs() {
        r.procs.insert(sig.oid, sig.name.to_string());
    }
    for f in db.functions.values() {
        r.procs.insert(f.oid, f.name.clone());
    }
    for s in db.schemas.values() {
        r.namespaces.insert(s.oid, s.name.clone());
    }
    r.roles.insert(10, user.to_string());
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
    // Operators are punctuation; function names start with a letter or _.
    if !name.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_') {
        if vals.iter().any(Value::is_null) {
            return Ok(Value::Null);
        }
        // A trailing `u` marks the prefix form, e.g. `-u` for unary minus.
        if vals.len() == 1 {
            return funcs::unop(name.strip_suffix('u').unwrap_or(name), &vals[0], ret);
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
            "PostgreSQL {} on x86_64-pc-linux-gnu, compiled by noida-db, 64-bit",
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
                    Some(e) => {
                        let path = ctx.rt.settings.lookup_path(&ctx.rt.user);
                        Value::text(ctx.db.regclass_text(e.schema, &e.name, &path))
                    }
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
            // is_local: for this transaction only, as SET LOCAL.
            if matches!(a.get(2), Some(Value::Bool(true))) {
                ctx.rt.local_settings.push(n.clone());
            }
            // The value as SHOW gives it (normalized: 'German' -> 'German, DMY').
            Value::text(ctx.rt.settings.get(&n).unwrap_or(v))
        }
        "pg_get_expr" => a[0].clone(),
        "pg_table_is_visible" => {
            match super::pgcatalog::relation_namespace(ctx.db, a[0].as_int().unwrap_or(0) as u32) {
                None => Value::Null,
                Some(ns) => {
                    let path = ctx.rt.settings.lookup_path(&ctx.rt.user);
                    Value::Bool(
                        ns == super::catalog::PG_CATALOG_NS
                            || path.iter().any(|s| ctx.db.schema_by_name(s) == Some(ns)),
                    )
                }
            }
        }
        "pg_type_is_visible"
        | "pg_function_is_visible"
        | "pg_collation_is_visible"
        | "pg_operator_is_visible"
        | "pg_opclass_is_visible"
        | "pg_conversion_is_visible"
        | "pg_ts_config_is_visible"
        | "pg_ts_dict_is_visible" => Value::Bool(true),
        "pg_get_userbyid" => Value::text(ctx.rt.user.clone()),
        "pg_encoding_to_char" | "getdatabaseencoding" => Value::text("UTF8"),
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
            // Sleeps in slices so a cancel or statement_timeout ends it.
            let secs = funcs::as_f64(&a[0]);
            let end = std::time::Instant::now()
                + std::time::Duration::from_secs_f64(if secs.is_finite() {
                    secs.clamp(0.0, 1e9)
                } else {
                    0.0
                });
            loop {
                ctx.rt.check_interrupt()?;
                let now = std::time::Instant::now();
                if now >= end {
                    break;
                }
                std::thread::sleep((end - now).min(std::time::Duration::from_millis(10)));
            }
            void()
        }
        "pg_advisory_lock"
        | "pg_advisory_xact_lock"
        | "pg_advisory_lock_shared"
        | "pg_advisory_unlock_all" => void(),
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
            pgcatalog::constraint_def(ctx.db, oid, &ctx.rt.settings.lookup_path(&ctx.rt.user))
                .map_or(Value::Null, Value::text)
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
            let path = ctx.rt.settings.lookup_path(&ctx.rt.user);
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
            if let Some(seq) = ctx.db.sequences.get(&oid)
                && (v < seq.min || v > seq.max)
            {
                return Err(PgError::new(
                    code::NUMERIC_VALUE_OUT_OF_RANGE,
                    format!(
                        "setval: value {v} is out of bounds for sequence \"{}\" ({}..{})",
                        seq.name, seq.min, seq.max
                    ),
                ));
            }
            let called = a.get(2).and_then(Value::as_bool).unwrap_or(true);
            ctx.seqs.insert(oid, SeqValue { last: v, is_called: called });
            ctx.rt.currval.insert(oid, v);
            ctx.rt.lastval = Some(oid);
            Value::Int(v)
        }
        "__enum_sortorder" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            match (&a[1], ctx.db.enums.get(&oid)) {
                (Value::Text(label), Some(e)) => e
                    .labels
                    .iter()
                    .find(|(_, l, _)| l == label)
                    .map_or(Value::Null, |(o, _, _)| Value::Float(*o as f64)),
                _ => Value::Null,
            }
        }
        "__enum_key" => {
            let rank = system_call("__enum_sortorder", a, tys, Type::FLOAT8, ctx)?;
            Value::Record(vec![rank, a[1].clone()])
        }
        "enum_range" | "enum_first" | "enum_last" => {
            let Some(Base::Enum(oid)) = tys.first().map(|t| t.base) else {
                return Err(PgError::new(
                    code::INVALID_PARAMETER_VALUE,
                    "could not determine actual enum type",
                ));
            };
            let Some(e) = ctx.db.enums.get(&oid) else { return Ok(Value::Null) };
            let mut labels = e.labels.clone();
            labels.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap_or(Ordering::Equal));
            let names: Vec<String> = labels.into_iter().map(|(_, l, _)| l).collect();
            match name {
                "enum_first" => names.first().cloned().map_or(Value::Null, Value::Text),
                "enum_last" => names.last().cloned().map_or(Value::Null, Value::Text),
                _ => {
                    // enum_range(lo, hi): the labels between them (NULL = open end).
                    let pos = |v: Option<&Value>| match v {
                        Some(Value::Text(l)) => names.iter().position(|n| n == l),
                        _ => None,
                    };
                    let lo = if a.len() > 1 { pos(a.first()).unwrap_or(0) } else { 0 };
                    let hi = if a.len() > 1 {
                        pos(a.get(1)).unwrap_or(names.len().saturating_sub(1))
                    } else {
                        names.len().saturating_sub(1)
                    };
                    let items: Vec<Value> = if names.is_empty() || lo > hi {
                        vec![]
                    } else {
                        names[lo..=hi].iter().cloned().map(Value::Text).collect()
                    };
                    Value::Array(Box::new(types::Array::new(items)))
                }
            }
        }
        "record_field" => {
            let idx = a[1].as_int().unwrap_or(0) as usize;
            match &a[0] {
                Value::Record(fields) => fields.get(idx).cloned().unwrap_or(Value::Null),
                other if idx == 0 => other.clone(),
                _ => Value::Null,
            }
        }
        "_pg_char_max_length"
        | "_pg_numeric_precision"
        | "_pg_numeric_scale"
        | "_pg_datetime_precision" => {
            let oid = a[0].as_int().unwrap_or(0) as u32;
            let typmod = a[1].as_int().unwrap_or(-1) as i32;
            let Some(ty) = Type::from_oid(oid) else { return Ok(Value::Null) };
            match name {
                "_pg_char_max_length" => match (ty.base, super::types::typmod_len(typmod)) {
                    (Base::Varchar | Base::Bpchar, Some(l)) => Value::Int(l as i64),
                    _ => Value::Null,
                },
                "_pg_numeric_precision" => match ty.base {
                    Base::Int2 => Value::Int(16),
                    Base::Int4 => Value::Int(32),
                    Base::Int8 => Value::Int(64),
                    Base::Float4 => Value::Int(24),
                    Base::Float8 => Value::Int(53),
                    Base::Numeric if typmod >= 4 => {
                        Value::Int((((typmod - 4) >> 16) & 0xffff) as i64)
                    }
                    _ => Value::Null,
                },
                "_pg_numeric_scale" => match ty.base {
                    Base::Int2 | Base::Int4 | Base::Int8 => Value::Int(0),
                    Base::Numeric if typmod >= 4 => Value::Int(((typmod - 4) & 0xffff) as i64),
                    _ => Value::Null,
                },
                _ => match ty.base {
                    Base::Date => Value::Int(0),
                    Base::Time
                    | Base::Timetz
                    | Base::Timestamp
                    | Base::Timestamptz
                    | Base::Interval => Value::Int(if typmod >= 0 { typmod as i64 } else { 6 }),
                    _ => Value::Null,
                },
            }
        }
        "_pg_truetypid" => a[1].clone(),
        "_pg_truetypmod" => Value::Int(-1),
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
                // A key on an object, an index on an array (a numeric
                // text key counts as one; an integer on an object is
                // that key), NULL otherwise.
                (Value::Jsonb(j), _) => {
                    use crate::sql::json::Json;
                    let found = match (j.as_ref(), &a[1]) {
                        (Json::Object(_), Value::Text(k)) => j.get(k),
                        (Json::Object(_), Value::Int(i)) => j.get(&i.to_string()),
                        (Json::Array(_), Value::Int(i)) => j.index(*i),
                        (Json::Array(_), Value::Text(k)) => {
                            k.trim().parse::<i64>().ok().and_then(|i| j.index(i))
                        }
                        _ => None,
                    };
                    found.cloned().map(|x| Value::Jsonb(Box::new(x))).unwrap_or(Value::Null)
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

/// The value of a `void` result: sent as an empty string, never NULL.
fn void() -> Value {
    Value::text("")
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
                    code::SEQUENCE_GENERATOR_LIMIT_EXCEEDED,
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
                    code::SEQUENCE_GENERATOR_LIMIT_EXCEEDED,
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
    ctx.subq_cache.push(Default::default());
    let r = run_query_inner(q, ctx);
    ctx.subq_cache.pop();
    r
}

fn run_query_inner(q: &Query, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
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
                // Rows are held (and copied) in memory, where Postgres
                // streams them: a runaway recursion stops here instead of
                // exhausting memory.
                if result.len() > MAX_RECURSIVE_ROWS {
                    return Err(PgError::new(code::OUT_OF_MEMORY, "out of memory").detail(format!(
                        "recursive query would return more than {MAX_RECURSIVE_ROWS} rows; noida-db materializes recursive queries"
                    )));
                }
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
    let mut rows = match filtered_scan(s, ctx)? {
        Some(rows) => return run_select_rest(s, rows, ctx),
        None => exec_from(&s.from, ctx)?,
    };
    if let Some(f) = &s.filter {
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if truthy(&eval(f, &r, ctx)?) {
                kept.push(r);
            }
        }
        rows = kept;
    }
    run_select_rest(s, rows, ctx)
}

/// One table with a WHERE that reads only its stored columns: the filter
/// runs on the stored rows and only the rows it keeps are copied (rather
/// than copying the whole table first).
fn filtered_scan(s: &Select, ctx: &mut Ctx) -> PgResult<Option<Vec<Row>>> {
    let (From::Table { oid, .. }, Some(f)) = (&s.from, &s.filter) else { return Ok(None) };
    let Some(t) = ctx.db.tables.get(oid).cloned() else { return Ok(None) };
    if !t.matview_populated || t.columns.iter().any(|c| c.dropped) {
        return Ok(None);
    }
    let width = t.columns.len();
    if f.contains(&|x| matches!(x, Expr::Col(i) if *i >= width)) {
        return Ok(None);
    }
    let mut kept = vec![];
    for (pos, r) in t.rows.iter().enumerate() {
        if truthy(&eval(f, r, ctx)?) {
            let mut row = r.clone();
            row.extend(t.system_col_values(pos));
            kept.push(row);
        }
    }
    Ok(Some(kept))
}

/// Everything in a SELECT after FROM and WHERE.
fn run_select_rest(s: &Select, mut rows: Vec<Row>, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    // Grouping and aggregation.
    if let (Some(keys), Some(sets)) = (&s.group, &s.grouping_sets) {
        // One aggregation per grouping set; keys outside the set are NULL.
        let mut out = vec![];
        for set in sets {
            let masked: Vec<Expr> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| if set.contains(&i) { k.clone() } else { Expr::Const(Value::Null) })
                .collect();
            let only_constants = masked.iter().all(|k| matches!(k, Expr::Const(_)));
            // The empty set is one group even over no rows.
            let part = if only_constants {
                let mut r = aggregate(&rows, &[], &s.aggs, ctx)?;
                for row in &mut r {
                    let mut full: Row = vec![Value::Null; keys.len()];
                    full.append(row);
                    *row = full;
                }
                r
            } else {
                aggregate(&rows, &masked, &s.aggs, ctx)?
            };
            out.extend(part);
        }
        rows = out;
    } else if let Some(keys) = &s.group {
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
    if s.with_ties {
        // FETCH FIRST n ROWS WITH TIES: also every following row that ties
        // the last one on the ORDER BY keys.
        let full = out.clone();
        let before = out.len();
        apply_limit(&mut out, &None, &s.offset, ctx)?;
        let skipped = before - out.len();
        apply_limit(&mut out, &s.limit, &None, ctx)?;
        if let Some(last) = out.last().cloned() {
            let key = |r: &Row| s.order.iter().map(|k| r[k.col].clone()).collect::<Vec<_>>();
            let last_key = key(&last);
            for r in full.iter().skip(skipped + out.len()) {
                if !rows_equal(&key(r), &last_key) {
                    break;
                }
                out.push(r.clone());
            }
        }
    } else {
        apply_limit(&mut out, &s.limit, &s.offset, ctx)?;
    }
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
        let mut columns: Vec<(Vec<Value>, bool)> = vec![];
        for e in proj {
            let is_srf = e.contains(&|x| {
                matches!(x, Expr::Call { name, .. } if super::sigs::kind_of(name) == Some(super::sigs::Kind::Srf))
            });
            columns.push((eval_multi(e, r, ctx)?, is_srf));
        }
        // Set-returning columns run in lockstep, the shorter ones padded
        // with NULLs; plain columns repeat.
        let n = columns.iter().filter(|c| c.1).map(|c| c.0.len()).max().unwrap_or(1);
        for i in 0..n {
            let mut row = vec![];
            for (c, srf) in &columns {
                row.push(if !srf {
                    c.first().cloned().unwrap_or(Value::Null)
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
    // `(srf(...)).field` expands the function, then takes one field.
    if let Expr::Call { name: "record_field", args, .. } = e
        && let [inner, Expr::Const(Value::Int(idx))] = args.as_slice()
        && matches!(inner, Expr::Call { name, .. } if super::sigs::kind_of(name) == Some(super::sigs::Kind::Srf))
    {
        let idx = *idx as usize;
        return Ok(eval_multi(inner, row, ctx)?
            .into_iter()
            .map(|v| match v {
                Value::Record(fields) => fields.get(idx).cloned().unwrap_or(Value::Null),
                other if idx == 0 => other,
                _ => Value::Null,
            })
            .collect());
    }
    let is_srf = |x: &Expr| matches!(x, Expr::Call { name, .. } if super::sigs::kind_of(name) == Some(super::sigs::Kind::Srf));
    if let Expr::Call { name, args, arg_tys, ty } = e
        && is_srf(e)
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
    // A set-returning call inside an expression (`unnest(a) * 10`,
    // `generate_series(...)::date`): expand it, then evaluate the rest of
    // the expression once per value.
    if e.contains(&is_srf) {
        let mut template = e.clone();
        let mut srf = None;
        take_first_srf(&mut template, &mut srf, &is_srf);
        if let Some(srf) = srf {
            let mut out = vec![];
            for v in eval_multi(&srf, row, ctx)? {
                let mut t = template.clone();
                fill_srf_slot(&mut t, &v);
                out.extend(eval_multi(&t, row, ctx)?);
            }
            return Ok(out);
        }
    }
    Ok(vec![eval(e, row, ctx)?])
}

/// The marker left where `take_first_srf` cut a set-returning call out.
const SRF_SLOT: &str = "\u{0}srf-slot";

fn take_first_srf(e: &mut Expr, found: &mut Option<Expr>, is_srf: &dyn Fn(&Expr) -> bool) {
    if found.is_some() {
        return;
    }
    if is_srf(e) {
        *found = Some(std::mem::replace(e, Expr::Const(Value::Text(SRF_SLOT.into()))));
        return;
    }
    e.children_mut(&mut |c| take_first_srf(c, found, is_srf));
}

fn fill_srf_slot(e: &mut Expr, v: &Value) {
    if matches!(e, Expr::Const(Value::Text(t)) if t == SRF_SLOT) {
        *e = Expr::Const(v.clone());
        return;
    }
    e.children_mut(&mut |c| fill_srf_slot(c, v));
}

fn exec_from(f: &From, ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    exec_from_with(f, ctx, &[])
}

/// `lat` is the row a lateral item to the left supplies: in
/// `FROM x, unnest(x.a) u JOIN t ON ...` the unnest is the left end of the
/// join to the right of the comma, and reads x's row.
fn exec_from_with(f: &From, ctx: &mut Ctx, lat: &[Value]) -> PgResult<Vec<Row>> {
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
            if !t.matview_populated {
                return Err(PgError::new(
                    code::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    format!("materialized view \"{}\" has not been populated", t.name),
                )
                .hint("Use the REFRESH MATERIALIZED VIEW command."));
            }
            // The binder numbers a table's columns without the dropped ones,
            // then appends `ctid`/`xmin`/`cmin`/`xmax`/`cmax`/`tableoid`.
            let has_dropped = t.columns.iter().any(|c| c.dropped);
            let live: Vec<usize> = t.live_columns().map(|(i, _)| i).collect();
            t.rows
                .iter()
                .enumerate()
                .map(|(pos, r)| {
                    let mut row: Row = if has_dropped {
                        live.iter().map(|&i| r[i].clone()).collect()
                    } else {
                        r.clone()
                    };
                    row.extend(t.system_col_values(pos));
                    row
                })
                .collect()
        }
        From::Virtual { name, .. } => pgcatalog::rows(name, ctx)?,
        From::Cte(slot) => ctx.ctes.get(*slot).cloned().flatten().unwrap_or_default(),
        From::Sub(q) => {
            ctx.outer.push(vec![]);
            let r = run_query(q, ctx);
            ctx.outer.pop();
            r?
        }
        From::Func { .. } => exec_func(f, lat, ctx)?,
        From::Join { left, right, kind, on, lateral, left_cols, right_cols } => {
            let lrows = exec_from_with(left, ctx, lat)?;
            let mut out = vec![];
            if *lateral {
                for l in &lrows {
                    ctx.outer.push(l.clone());
                    // The left row is the innermost outer row: a lateral
                    // subquery reads it as such, and a function's arguments
                    // are evaluated against it.
                    let rrows = match &**right {
                        From::Sub(q) => run_query(q, ctx),
                        f @ From::Func { .. } => exec_func(f, l, ctx),
                        other => exec_from_with(other, ctx, l),
                    };
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
            // With `l.a = r.b [AND ...]` in ON, only right rows whose keys
            // hash alike can match; ON still decides every candidate.
            let index = equi_join_index(on.as_ref(), *left_cols, &lrows, &rrows);
            let all: Vec<usize> = if index.is_none() { (0..rrows.len()).collect() } else { vec![] };
            for l in &lrows {
                let mut matched = false;
                let candidates = match &index {
                    Some((lkeys, map)) => match join_key(l, lkeys) {
                        Some(k) => map.get(&k).map(Vec::as_slice).unwrap_or(&[]),
                        None => &[],
                    },
                    None => &all[..],
                };
                for &j in candidates {
                    let mut row = l.clone();
                    row.extend(rrows[j].iter().cloned());
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

/// A set-returning function in FROM; `row` is the left-hand row a LATERAL
/// function's arguments refer to.
fn exec_func(f: &From, row: &[Value], ctx: &mut Ctx) -> PgResult<Vec<Row>> {
    let From::Func { name, args, arg_tys, ordinality, user, .. } = f else { return Ok(vec![]) };
    if let Some(oid) = user {
        let mut vals = vec![];
        for a in args {
            vals.push(eval(a, row, ctx)?);
        }
        let func = ctx.db.functions.get(oid).cloned().ok_or_else(|| {
            PgError::new(
                code::UNDEFINED_FUNCTION,
                format!("function with OID {oid} does not exist"),
            )
        })?;
        let info = super::dml::session_info(ctx);
        let v = super::plpgsql::call_function(ctx, &info, &func, vals)?;
        let mut rows = if func.returns_set { super::plpgsql::set_rows(v) } else { vec![vec![v]] };
        if *ordinality {
            for (i, r) in rows.iter_mut().enumerate() {
                r.push(Value::Int(i as i64 + 1));
            }
        }
        return Ok(rows);
    }
    // A scalar function in FROM (`SELECT * FROM current_schema()`) is valid
    // Postgres and returns one row of one column, not a set.
    if super::sigs::kind_of(name) != Some(super::sigs::Kind::Srf) {
        let ret = super::sigs::resolve(name, arg_tys).map(|r| r.ret).unwrap_or(Type::TEXT);
        return Ok(vec![vec![call_function(name, args, arg_tys, ret, row, ctx)?]]);
    }
    let mut vals = vec![];
    for a in args {
        vals.push(eval(a, row, ctx)?);
    }
    let mut rows = srf_rows(name, &vals, arg_tys, Type::TEXT, ctx)?;
    if *ordinality {
        for (i, r) in rows.iter_mut().enumerate() {
            r.push(Value::Int(i as i64 + 1));
        }
    }
    Ok(rows)
}

/// A join key column's value, such that values Postgres calls equal get
/// the same key (unequal ones may share one: ON still checks each pair).
#[derive(PartialEq, Eq, Hash)]
enum JoinKey {
    Bool(bool),
    /// Integers and numerics, as normalized decimal text.
    Exact(String),
    Text(String),
    Bytes(Vec<u8>),
    Int(i64),
    Uuid([u8; 16]),
}

fn join_key_value(v: &Value) -> Option<JoinKey> {
    Some(match v {
        Value::Bool(b) => JoinKey::Bool(*b),
        Value::Int(i) => JoinKey::Exact(i.to_string()),
        Value::Num(n) => {
            let s = n.to_string();
            let s =
                if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.') } else { &s };
            JoinKey::Exact(if s == "-0" { "0".into() } else { s.to_string() })
        }
        // bpchar comparison ignores trailing spaces.
        Value::Text(t) => JoinKey::Text(t.trim_end_matches(' ').to_string()),
        Value::Bytes(b) => JoinKey::Bytes(b.clone()),
        Value::Date(d) => JoinKey::Int(*d as i64),
        Value::Time(t) | Value::Ts(t) => JoinKey::Int(*t),
        Value::Uuid(u) => JoinKey::Uuid(*u),
        _ => return None,
    })
}

/// A row's key; `None` when a key column is NULL (no `=` can hold).
fn join_key(row: &[Value], cols: &[usize]) -> Option<Vec<JoinKey>> {
    let mut k = Vec::with_capacity(cols.len());
    for &c in cols {
        let v = row.get(c)?;
        if v.is_null() {
            return None;
        }
        k.push(join_key_value(v)?);
    }
    Some(k)
}

type JoinIndex = (Vec<usize>, std::collections::HashMap<Vec<JoinKey>, Vec<usize>>);

/// For an ON condition with `left.col = right.col` conjuncts: the left key
/// columns and the right rows by key. `None` (try every pair) when there
/// are no such conjuncts or a key value isn't hashable this way.
fn equi_join_index(
    on: Option<&Expr>,
    left_cols: usize,
    lrows: &[Row],
    rrows: &[Row],
) -> Option<JoinIndex> {
    fn conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::And(xs) => xs.iter().for_each(|x| conjuncts(x, out)),
            other => out.push(other),
        }
    }
    let mut cs = vec![];
    conjuncts(on?, &mut cs);
    let (mut lk, mut rk) = (vec![], vec![]);
    for c in cs {
        if let Expr::Compare { op: CmpOp::Eq, left, right, .. } = c
            && let (Expr::Col(a), Expr::Col(b)) = (&**left, &**right)
        {
            match (*a < left_cols, *b < left_cols) {
                (true, false) => {
                    lk.push(*a);
                    rk.push(*b - left_cols);
                }
                (false, true) => {
                    lk.push(*b);
                    rk.push(*a - left_cols);
                }
                _ => {}
            }
        }
    }
    if lk.is_empty() || lrows.len() * rrows.len() < 64 {
        return None;
    }
    // Floats compare equal to integers and numerics (via float8), which
    // these keys don't model: such joins try every pair.
    let unhashable = |rows: &[Row], cols: &[usize]| {
        rows.iter().any(|r| {
            cols.iter().any(|&c| {
                r.get(c).is_some_and(|v| {
                    !v.is_null() && (matches!(v, Value::Float(_)) || join_key_value(v).is_none())
                })
            })
        })
    };
    if unhashable(lrows, &lk) || unhashable(rrows, &rk) {
        return None;
    }
    let mut map: std::collections::HashMap<Vec<JoinKey>, Vec<usize>> = Default::default();
    for (j, r) in rrows.iter().enumerate() {
        if let Some(k) = join_key(r, &rk) {
            map.entry(k).or_default().push(j);
        }
    }
    Some((lk, map))
}

/// The most rows a recursive CTE may accumulate (it keeps copies of its
/// working set, so lower than `MAX_SRF_ROWS`).
const MAX_RECURSIVE_ROWS: usize = 1_000_000;

/// The most rows a set-returning function may produce: noida-db builds its
/// result in memory, where Postgres streams it.
const MAX_SRF_ROWS: usize = 10_000_000;

fn too_many_rows(func: &str) -> PgError {
    PgError::new(code::OUT_OF_MEMORY, "out of memory").detail(format!(
        "{func} would return more than {MAX_SRF_ROWS} rows; noida-db materializes set-returning functions"
    ))
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
                    let count = (*to as i128 - *from as i128) / step as i128 + 1;
                    if count > MAX_SRF_ROWS as i128 {
                        return Err(too_many_rows("generate_series"));
                    }
                    while (step > 0 && v <= *to) || (step < 0 && v >= *to) {
                        ctx.rt.tick()?;
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
                        if out.len() > MAX_SRF_ROWS {
                            return Err(too_many_rows("generate_series"));
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
                        if out.len() > MAX_SRF_ROWS {
                            return Err(too_many_rows("generate_series"));
                        }
                    }
                    out
                }
                _ => vec![],
            }
        }
        "_pg_expandarray" => match &a[0] {
            Value::Array(arr) => arr
                .items
                .iter()
                .enumerate()
                .map(|(i, v)| vec![v.clone(), Value::Int(i as i64 + 1)])
                .collect(),
            _ => vec![],
        },
        // No partitioned tables, so a relation is its own only ancestor.
        "pg_partition_ancestors" => one(vec![a[0].clone()]),
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
        "unnest" if a.len() > 1 => {
            let lists: Vec<&[Value]> = a
                .iter()
                .map(|v| match v {
                    Value::Array(arr) => arr.items.as_slice(),
                    _ => &[],
                })
                .collect();
            let n = lists.iter().map(|l| l.len()).max().unwrap_or(0);
            (0..n)
                .map(|i| lists.iter().map(|l| l.get(i).cloned().unwrap_or(Value::Null)).collect())
                .collect()
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
            let path = super::jsonpath::parse(match &a[1] {
                Value::Text(s) => s,
                _ => "",
            })?;
            let doc = match &a[0] {
                Value::Jsonb(j) => (**j).clone(),
                _ => crate::sql::json::Json::Null,
            };
            let vars = match a.get(2) {
                Some(Value::Jsonb(j)) => (**j).clone(),
                _ => crate::sql::json::Json::Object(vec![]),
            };
            let silent = a.get(3).and_then(Value::as_bool).unwrap_or(false);
            path.query(&doc, &vars, silent)?
                .into_iter()
                .map(|j| vec![Value::Jsonb(Box::new(j))])
                .collect()
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
        AggState::Values(items) => {
            // An ordered-set aggregate's direct argument (a fraction).
            let direct = match &agg.direct {
                Some(d) => Some(eval(d, &Vec::new(), ctx)?),
                None => None,
            };
            finish_collect(agg, items, &env, direct)?
        }
    })
}

fn finish_collect(
    agg: &AggCall,
    items: Vec<Value>,
    env: &Env,
    direct: Option<Value>,
) -> PgResult<Value> {
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
        "percentile_cont" | "percentile_disc" => {
            // Items arrive in the WITHIN GROUP order; NULLs don't count.
            let vals: Vec<&Value> = items.iter().filter(|v| !v.is_null()).collect();
            let at = |f: f64| -> PgResult<Value> {
                if !(0.0..=1.0).contains(&f) || f.is_nan() {
                    return Err(PgError::new(
                        code::NUMERIC_VALUE_OUT_OF_RANGE,
                        format!(
                            "percentile value {} is not between 0 and 1",
                            types::format_float(f, false, 1)
                        ),
                    ));
                }
                if vals.is_empty() {
                    return Ok(Value::Null);
                }
                let n = vals.len();
                if agg.name == "percentile_disc" {
                    let i = ((f * n as f64).ceil() as usize).max(1) - 1;
                    return Ok(vals[i.min(n - 1)].clone());
                }
                let pos = f * (n - 1) as f64;
                let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
                let (a, b) = (funcs::as_f64(vals[lo]), funcs::as_f64(vals[hi]));
                Ok(Value::Float(a + (b - a) * (pos - lo as f64)))
            };
            match direct {
                Some(Value::Array(fs)) => {
                    let mut out = vec![];
                    for f in &fs.items {
                        out.push(if f.is_null() { Value::Null } else { at(funcs::as_f64(f))? });
                    }
                    Value::Array(Box::new(Array::new(out)))
                }
                Some(Value::Null) | None => Value::Null,
                Some(f) => at(funcs::as_f64(&f))?,
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
        /// Inputs of aggregates with ORDER BY: (sort keys, arguments).
        ordered: Vec<Vec<(Vec<Value>, Vec<Value>)>>,
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
                    ordered: vec![vec![]; aggs.len()],
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
            // DISTINCT without ORDER BY: Postgres sorts the input to drop
            // duplicates, so the aggregate sees it in ascending order.
            if !agg.order.is_empty() || agg.distinct {
                let mut keys = Vec::with_capacity(agg.order.len());
                for (e, _, _) in &agg.order {
                    keys.push(eval(e, r, ctx)?);
                }
                if agg.order.is_empty() {
                    keys = vals.iter().take(1).cloned().collect();
                }
                groups[idx].ordered[i].push((keys, vals));
                continue;
            }
            let g = &mut groups[idx];
            accumulate(&mut g.states[i], agg, &vals, &mut g.counts[i])?;
        }
    }
    // Aggregates with ORDER BY see their inputs in that order.
    for g in &mut groups {
        for (i, agg) in aggs.iter().enumerate() {
            if agg.order.is_empty() && !agg.distinct {
                continue;
            }
            let mut inputs = std::mem::take(&mut g.ordered[i]);
            if agg.order.is_empty() {
                inputs.sort_by(|a, b| types::cmp_values(&a.0[0], &b.0[0]));
            }
            inputs.sort_by(|a, b| {
                for (k, (_, desc, nulls_first)) in agg.order.iter().enumerate() {
                    let o = match (a.0[k].is_null(), b.0[k].is_null()) {
                        (true, true) => Ordering::Equal,
                        (true, false) if *nulls_first => Ordering::Less,
                        (true, false) => Ordering::Greater,
                        (false, true) if *nulls_first => Ordering::Greater,
                        (false, true) => Ordering::Less,
                        _ => {
                            let c = types::cmp_values(&a.0[k], &b.0[k]);
                            if *desc { c.reverse() } else { c }
                        }
                    };
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                Ordering::Equal
            });
            for (_, vals) in inputs {
                accumulate(&mut g.states[i], agg, &vals, &mut g.counts[i])?;
            }
        }
    }
    // An aggregate with no GROUP BY over no rows still returns one row.
    if groups.is_empty() && keys.is_empty() {
        groups.push(Group {
            key: vec![],
            states: aggs.iter().map(new_state).collect(),
            counts: vec![0; aggs.len()],
            seen: vec![],
            ordered: vec![vec![]; aggs.len()],
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
                let (start, end) = frame_bounds(w, p, n, ctx, &rows[idxs[p]], &order_keys)?;
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
                let (start, end) = frame_bounds(w, p, n, ctx, &rows[idxs[p]], &order_keys)?;
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
    keys: &[Vec<Value>],
) -> PgResult<(usize, usize)> {
    let peer = |a: usize, b: usize| !w.order.is_empty() && rows_equal(&keys[a], &keys[b]);
    let peer_start = |p: usize| {
        let mut s = p;
        while s > 0 && peer(s - 1, p) {
            s -= 1;
        }
        s
    };
    let peer_end = |p: usize| {
        let mut e = p + 1;
        while e < n && peer(p, e) {
            e += 1;
        }
        e
    };
    // No frame clause: RANGE UNBOUNDED PRECEDING .. CURRENT ROW, peers
    // included (the whole partition without ORDER BY).
    let Some(f) = &w.frame else {
        return Ok(if w.order.is_empty() { (0, n) } else { (0, peer_end(p)) });
    };
    let count = |e: &Expr, ctx: &mut Ctx| -> PgResult<i64> {
        let v = eval(e, row, ctx)?;
        match v.as_int() {
            Some(k) if k >= 0 => Ok(k),
            Some(_) => Err(PgError::new(
                code::INVALID_PRECEDING_OR_FOLLOWING_SIZE,
                "frame starting offset must not be negative",
            )),
            None if v.is_null() => Err(PgError::new(
                code::NULL_VALUE_NOT_ALLOWED,
                "frame starting offset must not be null",
            )),
            None => Ok(0),
        }
    };
    if f.rows {
        let start = match &f.start {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::CurrentRow => p,
            FrameBound::Preceding(e) => p.saturating_sub(count(e, ctx)? as usize),
            FrameBound::Following(e) => p + count(e, ctx)? as usize,
            FrameBound::UnboundedFollowing => n,
        };
        let end = match &f.end {
            FrameBound::UnboundedFollowing => n,
            FrameBound::CurrentRow => p + 1,
            FrameBound::Following(e) => (p + count(e, ctx)? as usize + 1).min(n),
            FrameBound::Preceding(e) => (p + 1).saturating_sub(count(e, ctx)? as usize),
            FrameBound::UnboundedPreceding => 0,
        };
        return Ok((start.min(n), end.min(n)));
    }
    if f.groups {
        // Peer-group numbers and their [start, end) ranges.
        let mut group_of = vec![0usize; n];
        let mut bounds: Vec<(usize, usize)> = vec![];
        let mut i = 0;
        while i < n {
            let e = peer_end(i);
            for g in group_of.iter_mut().take(e).skip(i) {
                *g = bounds.len();
            }
            bounds.push((i, e));
            i = e;
        }
        let g = group_of[p] as i64;
        let last = bounds.len() as i64 - 1;
        let at = |k: i64| bounds[k.clamp(0, last) as usize];
        let start = match &f.start {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::CurrentRow => at(g).0,
            FrameBound::Preceding(e) => at((g - count(e, ctx)?).max(0)).0,
            FrameBound::Following(e) => {
                let k = g + count(e, ctx)?;
                if k > last { n } else { at(k).0 }
            }
            FrameBound::UnboundedFollowing => n,
        };
        let end = match &f.end {
            FrameBound::UnboundedFollowing => n,
            FrameBound::CurrentRow => at(g).1,
            FrameBound::Following(e) => at((g + count(e, ctx)?).min(last)).1,
            FrameBound::Preceding(e) => {
                let k = g - count(e, ctx)?;
                if k < 0 { 0 } else { at(k).1 }
            }
            FrameBound::UnboundedPreceding => 0,
        };
        return Ok((start, end));
    }
    // RANGE: offsets are distances in the (single) ORDER BY key's values.
    let desc = w.order.first().is_some_and(|o| o.1);
    let key = |q: usize| keys[q].first().cloned().unwrap_or(Value::Null);
    let cur = key(p);
    let (fmt, now, stmt_now) = env!(ctx);
    let env = Env { fmt: &fmt, now, stmt_now };
    // `key - off` / `key + off`, comparable with the other keys.
    let shift = |off: &Value, plus: bool| -> PgResult<Value> {
        let k = match &cur {
            Value::Date(d) => Value::Ts(super::casts::date_to_ts(*d)),
            other => other.clone(),
        };
        match (&k, off) {
            (Value::Ts(_), Value::Interval(_)) => funcs::binop(
                if plus { "+" } else { "-" },
                &k,
                off,
                Type::TIMESTAMP,
                &[Type::TIMESTAMP, Type::INTERVAL],
                &env,
            ),
            _ => {
                let (Some(x), Some(y)) = (num_f64(&k), num_f64(off)) else {
                    return Err(PgError::new(
                        code::FEATURE_NOT_SUPPORTED,
                        "RANGE with offset PRECEDING/FOLLOWING is not supported for this column type",
                    ));
                };
                if y < 0.0 {
                    return Err(PgError::new(
                        code::INVALID_PRECEDING_OR_FOLLOWING_SIZE,
                        "invalid preceding or following size in window function",
                    ));
                }
                Ok(Value::Float(if plus { x + y } else { x - y }))
            }
        }
    };
    let cmp = |q: usize, b: &Value| -> Ordering {
        let k = match key(q) {
            Value::Date(d) => Value::Ts(super::casts::date_to_ts(d)),
            other => other,
        };
        match (num_f64(&k), num_f64(b)) {
            (Some(x), Some(y)) if !matches!(k, Value::Ts(_)) => {
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
            _ => types::cmp_values(&k, b),
        }
    };
    // "Before the bound" in sort order: smaller for ASC, larger for DESC.
    let before = |q: usize, b: &Value| {
        let c = cmp(q, b);
        if desc { c == Ordering::Greater } else { c == Ordering::Less }
    };
    let after = |q: usize, b: &Value| {
        let c = cmp(q, b);
        if desc { c == Ordering::Less } else { c == Ordering::Greater }
    };
    let offset_bound = |e: &Expr, toward_start: bool, ctx: &mut Ctx| -> PgResult<Option<Value>> {
        if cur.is_null() {
            return Ok(None);
        }
        let off = eval(e, row, ctx)?;
        // PRECEDING moves toward the start of the sort order.
        let plus = toward_start == desc;
        Ok(Some(shift(&off, plus)?))
    };
    let start = match &f.start {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::CurrentRow => peer_start(p),
        FrameBound::UnboundedFollowing => n,
        FrameBound::Preceding(e) | FrameBound::Following(e) => {
            let toward_start = matches!(f.start, FrameBound::Preceding(_));
            match offset_bound(e, toward_start, ctx)? {
                None => peer_start(p),
                Some(b) => (0..n).find(|&q| !key(q).is_null() && !before(q, &b)).unwrap_or(n),
            }
        }
    };
    let end = match &f.end {
        FrameBound::UnboundedFollowing => n,
        FrameBound::CurrentRow => peer_end(p),
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(e) | FrameBound::Following(e) => {
            let toward_start = matches!(f.end, FrameBound::Preceding(_));
            match offset_bound(e, toward_start, ctx)? {
                None => peer_end(p),
                Some(b) => {
                    (0..n).rev().find(|&q| !key(q).is_null() && !after(q, &b)).map_or(0, |q| q + 1)
                }
            }
        }
    };
    Ok((start, end.max(start)))
}

fn num_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Num(n) => Some(n.to_f64()),
        _ => None,
    }
}
