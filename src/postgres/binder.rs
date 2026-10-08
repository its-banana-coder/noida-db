//! Parse analysis: resolves names and types in a sqlparser AST and produces
//! the typed plans in [`super::plan`]. Errors match Postgres's SQLSTATEs.

use sqlparser::ast as a;

use super::casts::{self, CastCtx};
use super::catalog::{DbState, RelKind, Table};
use super::error::{PgError, PgResult, code};
use super::pgcatalog;
use super::plan::*;
use super::sigs::{self, Kind};
use super::types::{self, Base, FmtCtx, Type, Value};

/// What the binder needs to know about the session.
pub struct SessionInfo {
    /// The session user (login).
    pub user: String,
    /// The current role (SET ROLE), what current_user reports.
    pub role: String,
    pub database: String,
    pub search_path: Vec<String>,
    pub fmt: FmtCtx,
    /// Transaction start time (now()).
    pub now: i64,
}

#[derive(Clone, Debug)]
struct SCol {
    rel: Option<String>,
    name: String,
    ty: Type,
    typmod: i32,
    idx: usize,
    table_oid: u32,
    attnum: i16,
    /// USING/NATURAL hides the underlying columns behind a merged one, or
    /// (with `system`) this is a system column hidden from `SELECT *`.
    hidden: bool,
    /// A system column (`ctid`/`xmin`/...): still resolves by bare name
    /// (unlike other `hidden` columns, e.g. a pre-merge USING/NATURAL join
    /// column or `excluded.*` in `ON CONFLICT`, which must be qualified).
    system: bool,
    /// Field names of a record-typed column.
    rec: Option<Vec<(String, Type)>>,
}

#[derive(Clone, Debug)]
struct Merged {
    name: String,
    expr: Expr,
    ty: Type,
    typmod: i32,
}

#[derive(Clone, Debug, Default)]
struct Scope {
    cols: Vec<SCol>,
    merged: Vec<Merged>,
    rels: Vec<String>,
}

impl Scope {
    fn width(&self) -> usize {
        self.cols.len()
    }
}

#[derive(Clone, Debug)]
struct CteDef {
    name: String,
    slot: usize,
    cols: Vec<OutCol>,
}

#[derive(Default)]
struct AggFrame {
    aggs: Vec<AggCall>,
    wins: Vec<WinCall>,
    /// Where aggregates aren't allowed, the clause's name for the error.
    forbid: Option<&'static str>,
}

pub struct Binder<'a> {
    /// Window function calls being bound (their arguments may hold
    /// aggregates of the query, but not another window call).
    window_depth: usize,
    pub db: &'a DbState,
    pub sess: &'a SessionInfo,
    /// Parameter types; `unknown` until resolved from context.
    pub params: Vec<Type>,
    scopes: Vec<Scope>,
    ctes: Vec<CteDef>,
    cte_level: usize,
    pub cte_slots: usize,
    frames: Vec<AggFrame>,
    /// The current SELECT's `WINDOW name AS (...)` definitions.
    named_windows: Vec<a::NamedWindowDefinition>,
    /// Views being expanded, innermost last (a cycle is an error, not a
    /// stack overflow).
    expanding_views: Vec<u32>,
}

fn ident(id: &a::Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_lowercase(),
    }
}

fn object_name(n: &a::ObjectName) -> Vec<String> {
    n.0.iter()
        .map(|p| match p {
            a::ObjectNamePart::Identifier(i) => ident(i),
            a::ObjectNamePart::Function(f) => ident(&f.name),
        })
        .collect()
}

pub fn name_parts(n: &a::ObjectName) -> Vec<String> {
    object_name(n)
}

fn syntax(msg: impl Into<String>) -> PgError {
    PgError::new(code::SYNTAX_ERROR, msg)
}

pub fn unsupported(what: &str) -> PgError {
    PgError::new(code::FEATURE_NOT_SUPPORTED, format!("{what} is not supported"))
}

impl<'a> Binder<'a> {
    pub fn new(db: &'a DbState, sess: &'a SessionInfo, param_hints: &[Type]) -> Binder<'a> {
        Binder {
            window_depth: 0,
            db,
            sess,
            params: param_hints.to_vec(),
            scopes: vec![],
            ctes: vec![],
            cte_level: 0,
            cte_slots: 0,
            frames: vec![],
            named_windows: vec![],
            expanding_views: vec![],
        }
    }

    fn fmt(&self) -> &FmtCtx {
        &self.sess.fmt
    }

    fn env(&self) -> super::funcs::Env<'_> {
        super::funcs::Env { fmt: self.fmt(), now: self.sess.now, stmt_now: self.sess.now }
    }

    // -----------------------------------------------------------------
    // Statements

    /// Plans a SELECT or a data-modifying statement.
    pub fn bind_statement(&mut self, stmt: &a::Statement) -> PgResult<Planned> {
        match stmt {
            a::Statement::Query(q) => {
                let (mut query, mut cols) = self.bind_query(q)?;
                resolve_unknown_output(&mut query, &mut cols);
                Ok(Planned {
                    query,
                    cols,
                    tag: "SELECT",
                    cte_slots: self.cte_slots,
                    returns_rows: true,
                })
            }
            a::Statement::Insert(ins) => self.bind_insert(ins),
            a::Statement::Update(up) => self.bind_update(up),
            a::Statement::Delete(del) => self.bind_delete(del),
            other => Err(unsupported(&format!("statement {}", first_words(&other.to_string())))),
        }
    }

    // -----------------------------------------------------------------
    // Queries

    pub fn bind_query(&mut self, q: &a::Query) -> PgResult<(Query, Vec<OutCol>)> {
        let ctes_before = self.ctes.len();
        let mut cte_plans = vec![];
        if let Some(with) = &q.with {
            self.cte_level += 1;
            for cte in &with.cte_tables {
                let name = ident(&cte.alias.name);
                let slot = self.cte_slots;
                self.cte_slots += 1;
                if with.recursive && query_references(&cte.query, &name) {
                    let plan = self.bind_recursive_cte(cte, slot, &name)?;
                    cte_plans.push(plan);
                } else {
                    let (mut query, mut cols) = self.bind_query(&cte.query)?;
                    resolve_unknown_output(&mut query, &mut cols);
                    rename_cols(&mut cols, &cte.alias.columns, &name)?;
                    self.ctes.push(CteDef { name, slot, cols });
                    cte_plans.push(CtePlan { slot, query, recursive: false });
                }
            }
        }
        let (body, cols) = self.bind_body(&q.body, q)?;
        self.ctes.truncate(ctes_before);
        if q.with.is_some() {
            self.cte_level -= 1;
        }
        let query = if cte_plans.is_empty() {
            body
        } else {
            Query::With { ctes: cte_plans, body: Box::new(body) }
        };
        Ok((query, cols))
    }

    fn bind_recursive_cte(&mut self, cte: &a::Cte, slot: usize, name: &str) -> PgResult<CtePlan> {
        // WITH RECURSIVE x AS (seed UNION [ALL] step)
        let a::SetExpr::SetOperation { left, op: a::SetOperator::Union, set_quantifier, right } =
            cte.query.body.as_ref()
        else {
            return Err(PgError::new(
                code::SYNTAX_ERROR,
                format!(
                    "recursive query \"{name}\" does not have the form non-recursive-term UNION [ALL] recursive-term"
                ),
            ));
        };
        let all = matches!(set_quantifier, a::SetQuantifier::All);
        let (seed, mut cols) = self.bind_set_expr(left)?;
        rename_cols(&mut cols, &cte.alias.columns, name)?;
        self.ctes.push(CteDef { name: name.to_string(), slot, cols });
        let (step, step_cols) = self.bind_set_expr(right)?;
        let seed_cols = self.ctes.last().unwrap().cols.clone();
        if step_cols.len() != seed_cols.len() {
            return Err(syntax("each UNION query must have the same number of columns"));
        }
        Ok(CtePlan {
            slot,
            query: Query::Recursive { slot, seed: Box::new(seed), step: Box::new(step), all },
            recursive: true,
        })
    }

    fn bind_body(&mut self, body: &a::SetExpr, q: &a::Query) -> PgResult<(Query, Vec<OutCol>)> {
        match body {
            a::SetExpr::Select(sel) => self.bind_select(sel, q),
            a::SetExpr::Query(inner) => self.bind_query(inner),
            a::SetExpr::Values(v) => {
                let (rows, cols) = self.bind_values(v)?;
                let (order, limit, offset) = self.bind_tail(q, &cols, None)?;
                Ok((Query::Values { rows, order, limit, offset }, cols))
            }
            a::SetExpr::SetOperation { .. } => {
                let (sq, cols) = self.bind_set_expr(body)?;
                let (order, limit, offset) = self.bind_tail(q, &cols, None)?;
                Ok((attach_tail(sq, order, limit, offset), cols))
            }
            // A data-modifying statement inside WITH (`WITH d AS (DELETE ...
            // RETURNING *) SELECT ...`). Found via testing before a public
            // release.
            a::SetExpr::Insert(stmt) | a::SetExpr::Update(stmt) | a::SetExpr::Delete(stmt) => {
                let planned = match stmt {
                    a::Statement::Insert(ins) => self.bind_insert(ins)?,
                    a::Statement::Update(up) => self.bind_update(up)?,
                    a::Statement::Delete(del) => self.bind_delete(del)?,
                    other => return Err(unsupported(&first_words(&other.to_string()))),
                };
                Ok((planned.query, planned.cols))
            }
            other => Err(unsupported(&first_words(&other.to_string()))),
        }
    }

    fn bind_set_expr(&mut self, body: &a::SetExpr) -> PgResult<(Query, Vec<OutCol>)> {
        match body {
            a::SetExpr::Select(sel) => {
                let empty = a::Query {
                    with: None,
                    body: Box::new(body.clone()),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                };
                self.bind_select(sel, &empty)
            }
            a::SetExpr::Query(inner) => self.bind_query(inner),
            a::SetExpr::Values(v) => {
                let (rows, cols) = self.bind_values(v)?;
                Ok((Query::Values { rows, order: vec![], limit: None, offset: None }, cols))
            }
            a::SetExpr::SetOperation { left, op, set_quantifier, right } => {
                let (mut lq, lcols) = self.bind_set_expr(left)?;
                let (mut rq, rcols) = self.bind_set_expr(right)?;
                let opname = match op {
                    a::SetOperator::Union => "UNION",
                    a::SetOperator::Except => "EXCEPT",
                    a::SetOperator::Intersect => "INTERSECT",
                    a::SetOperator::Minus => return Err(unsupported("MINUS")),
                };
                if lcols.len() != rcols.len() {
                    return Err(syntax(format!(
                        "each {opname} query must have the same number of columns"
                    )));
                }
                let mut cols = vec![];
                let mut lcast = vec![];
                let mut rcast = vec![];
                for (i, (l, r)) in lcols.iter().zip(&rcols).enumerate() {
                    let ty = self.common_type(&[l.ty, r.ty], opname, i)?;
                    lcast.push(ty);
                    rcast.push(ty);
                    cols.push(OutCol {
                        name: l.name.clone(),
                        ty,
                        typmod: if l.typmod == r.typmod { l.typmod } else { -1 },
                        table_oid: 0,
                        attnum: 0,
                        rec: None,
                    });
                }
                lq = self.cast_query_columns(lq, &lcols, &lcast)?;
                rq = self.cast_query_columns(rq, &rcols, &rcast)?;
                let all = matches!(set_quantifier, a::SetQuantifier::All);
                let kind = match op {
                    a::SetOperator::Union => SetOpKind::Union,
                    a::SetOperator::Except => SetOpKind::Except,
                    _ => SetOpKind::Intersect,
                };
                Ok((
                    Query::SetOp {
                        op: kind,
                        all,
                        left: Box::new(lq),
                        right: Box::new(rq),
                        order: vec![],
                        limit: None,
                        offset: None,
                    },
                    cols,
                ))
            }
            other => Err(unsupported(&first_words(&other.to_string()))),
        }
    }

    /// Wraps a query so its columns have the given types.
    fn cast_query_columns(
        &mut self,
        q: Query,
        cols: &[OutCol],
        target: &[Type],
    ) -> PgResult<Query> {
        if cols.iter().zip(target).all(|(c, t)| c.ty == *t) {
            return Ok(q);
        }
        let proj = cols
            .iter()
            .zip(target)
            .enumerate()
            .map(|(i, (c, t))| {
                if c.ty == *t {
                    Expr::Col(i)
                } else {
                    Expr::Cast {
                        expr: Box::new(Expr::Col(i)),
                        from: c.ty,
                        to: *t,
                        typmod: -1,
                        explicit: false,
                    }
                }
            })
            .collect();
        Ok(Query::Select(Box::new(Select {
            from: From::Sub(Box::new(q)),
            filter: None,
            group: None,
            grouping_sets: None,
            aggs: vec![],
            having: None,
            windows: vec![],
            proj,
            visible: cols.len(),
            distinct: Distinct::None,
            order: vec![],
            limit: None,
            offset: None,
            with_ties: false,
            srf: false,
        })))
    }

    fn bind_values(&mut self, v: &a::Values) -> PgResult<(Vec<Vec<Expr>>, Vec<OutCol>)> {
        let mut rows: Vec<Vec<TE>> = vec![];
        let width = v.rows.first().map_or(0, |r| r.content.len());
        self.frames.push(AggFrame { forbid: Some("VALUES"), ..Default::default() });
        self.scopes.push(Scope::default());
        for row in &v.rows {
            if row.content.len() != width {
                return Err(PgError::new(
                    code::SYNTAX_ERROR,
                    "VALUES lists must all be the same length",
                ));
            }
            let mut out = vec![];
            for e in &row.content {
                out.push(self.bind_expr(e)?);
            }
            rows.push(out);
        }
        self.scopes.pop();
        self.frames.pop();
        let mut cols = vec![];
        for i in 0..width {
            let tys: Vec<Type> = rows.iter().map(|r| r[i].ty).collect();
            let ty = self.common_type(&tys, "VALUES", i)?;
            cols.push(OutCol::new(format!("column{}", i + 1), ty));
        }
        let mut out_rows = vec![];
        for row in rows {
            let mut r = vec![];
            for (i, te) in row.into_iter().enumerate() {
                r.push(self.coerce(te, cols[i].ty, -1, CastCtx::Implicit, "VALUES")?);
            }
            out_rows.push(r);
        }
        Ok((out_rows, cols))
    }

    /// ORDER BY / LIMIT / OFFSET shared by set operations and VALUES.
    fn bind_tail(
        &mut self,
        q: &a::Query,
        cols: &[OutCol],
        _sel: Option<()>,
    ) -> PgResult<(Vec<SortKey>, Option<Expr>, Option<Expr>)> {
        let mut order = vec![];
        if let Some(ob) = &q.order_by {
            let a::OrderByKind::Expressions(exprs) = &ob.kind else {
                return Err(unsupported("ORDER BY ALL"));
            };
            for o in exprs {
                let col = match &o.expr {
                    a::Expr::Value(v) => match &v.value {
                        a::Value::Number(n, _) => {
                            let idx: usize =
                                n.parse().map_err(|_| syntax("invalid ORDER BY position"))?;
                            if idx < 1 || idx > cols.len() {
                                return Err(PgError::new(
                                    code::INVALID_COLUMN_REFERENCE,
                                    format!("ORDER BY position {idx} is not in select list"),
                                ));
                            }
                            idx - 1
                        }
                        _ => return Err(unsupported("ORDER BY expression over a set operation")),
                    },
                    a::Expr::Identifier(id) => {
                        let n = ident(id);
                        cols.iter().position(|c| c.name == n).ok_or_else(|| {
                            PgError::new(
                                code::UNDEFINED_COLUMN,
                                format!("column \"{n}\" does not exist"),
                            )
                        })?
                    }
                    _ => return Err(unsupported("ORDER BY expression over a set operation")),
                };
                order.push(SortKey {
                    col,
                    desc: o.options.sort == Some(a::OrderBySort::Desc),
                    nulls_first: o
                        .options
                        .nulls_first
                        .unwrap_or(o.options.sort == Some(a::OrderBySort::Desc)),
                });
            }
        }
        let (limit, offset) = self.bind_limit(q)?;
        Ok((order, limit, offset))
    }

    fn bind_limit(&mut self, q: &a::Query) -> PgResult<(Option<Expr>, Option<Expr>)> {
        let mut limit = None;
        let mut offset = None;
        self.scopes.push(Scope::default());
        self.frames.push(AggFrame { forbid: Some("LIMIT"), ..Default::default() });
        let r = (|| -> PgResult<()> {
            if let Some(lc) = &q.limit_clause {
                match lc {
                    a::LimitClause::LimitOffset { limit: l, offset: o, limit_by } => {
                        if !limit_by.is_empty() {
                            return Err(unsupported("LIMIT BY"));
                        }
                        if let Some(l) = l {
                            let te = self.bind_expr(l)?;
                            limit = Some(self.coerce(
                                te,
                                Type::INT8,
                                -1,
                                CastCtx::Assignment,
                                "LIMIT",
                            )?);
                        }
                        if let Some(o) = o {
                            let te = self.bind_expr(&o.value)?;
                            offset = Some(self.coerce(
                                te,
                                Type::INT8,
                                -1,
                                CastCtx::Assignment,
                                "OFFSET",
                            )?);
                        }
                    }
                    a::LimitClause::OffsetCommaLimit { offset: o, limit: l } => {
                        let te = self.bind_expr(l)?;
                        limit =
                            Some(self.coerce(te, Type::INT8, -1, CastCtx::Assignment, "LIMIT")?);
                        let te = self.bind_expr(o)?;
                        offset =
                            Some(self.coerce(te, Type::INT8, -1, CastCtx::Assignment, "OFFSET")?);
                    }
                }
            }
            if let Some(f) = &q.fetch
                && f.with_ties
                && q.order_by.is_none()
            {
                return Err(PgError::new(
                    code::SYNTAX_ERROR,
                    "WITH TIES cannot be specified without ORDER BY clause",
                ));
            }
            if let Some(f) = &q.fetch
                && let Some(qty) = &f.quantity
            {
                let te = self.bind_expr(qty)?;
                limit = Some(self.coerce(te, Type::INT8, -1, CastCtx::Assignment, "LIMIT")?);
            }
            Ok(())
        })();
        self.frames.pop();
        self.scopes.pop();
        r?;
        Ok((limit, offset))
    }

    // -----------------------------------------------------------------
    // SELECT

    fn bind_select(&mut self, sel: &a::Select, q: &a::Query) -> PgResult<(Query, Vec<OutCol>)> {
        for (what, used) in [
            ("DISTRIBUTE BY", !sel.distribute_by.is_empty()),
            ("CLUSTER BY", !sel.cluster_by.is_empty()),
            ("QUALIFY", sel.qualify.is_some()),
            ("PREWHERE", sel.prewhere.is_some()),
            ("SELECT INTO", sel.into.is_some()),
            ("CONNECT BY", !sel.connect_by.is_empty()),
        ] {
            if used {
                return Err(unsupported(what));
            }
        }
        // FROM
        let (from, scope) = self.bind_from(&sel.from)?;
        self.scopes.push(scope);
        let saved = std::mem::replace(&mut self.named_windows, sel.named_window.clone());
        let result = self.bind_select_body(sel, q, from);
        self.named_windows = saved;
        self.scopes.pop();
        result
    }

    /// A window spec with any named window it refers to (`OVER w`,
    /// `OVER (w ORDER BY x)`) merged in, as Postgres does: the reference
    /// supplies PARTITION BY, and ORDER BY or the frame when the new spec
    /// leaves them out.
    fn resolve_window(&self, over: &a::WindowType) -> PgResult<a::WindowSpec> {
        let (spec, base) = match over {
            a::WindowType::WindowSpec(s) => (s.clone(), s.window_name.clone()),
            a::WindowType::NamedWindow(n) => (
                a::WindowSpec {
                    window_name: None,
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: None,
                },
                Some(n.clone()),
            ),
        };
        let Some(base) = base else { return Ok(spec) };
        let def = self.named_windows.iter().find(|d| d.0.value == base.value).ok_or_else(|| {
            PgError::new(
                code::UNDEFINED_OBJECT,
                format!("window \"{}\" does not exist", base.value),
            )
        })?;
        let base_spec = match &def.1 {
            a::NamedWindowExpr::WindowSpec(s) => {
                self.resolve_window(&a::WindowType::WindowSpec(s.clone()))?
            }
            a::NamedWindowExpr::NamedWindow(n) => {
                self.resolve_window(&a::WindowType::NamedWindow(n.clone()))?
            }
        };
        if !spec.partition_by.is_empty() {
            return Err(PgError::new(
                code::WINDOWING_ERROR,
                format!("cannot override PARTITION BY clause of window \"{}\"", base.value),
            ));
        }
        Ok(a::WindowSpec {
            window_name: None,
            partition_by: base_spec.partition_by,
            order_by: if spec.order_by.is_empty() { base_spec.order_by } else { spec.order_by },
            window_frame: spec.window_frame.or(base_spec.window_frame),
        })
    }

    fn bind_select_body(
        &mut self,
        sel: &a::Select,
        q: &a::Query,
        mut from: From,
    ) -> PgResult<(Query, Vec<OutCol>)> {
        let input_width = self.scopes.last().unwrap().width();
        // WHERE
        let mut filter = match &sel.selection {
            Some(e) => {
                self.frames.push(AggFrame { forbid: Some("WHERE"), ..Default::default() });
                let te = self.bind_expr(e);
                self.frames.pop();
                Some(self.bool_expr(te?, "WHERE")?)
            }
            None => None,
        };
        // A comma-separated `FROM a, b, c` binds to a chain of `Cross`
        // joins with no `on`, which without this would run as a fully
        // unfiltered nested-loop join at every step (many ORMs' catalog
        // queries still use this older join style, and it blows up
        // memory/time on wide implicit joins once WHERE is the only
        // filter). Push down whichever WHERE conjuncts become fully
        // evaluable once a join's columns are all in scope.
        if let Some(f) = filter.take() {
            let mut remaining = vec![];
            flatten_and(f, &mut remaining);
            push_cross_predicates(&mut from, &mut remaining);
            filter = if remaining.is_empty() { None } else { Some(and_all(remaining)) };
        }
        // Everything below may contain aggregates and window functions.
        self.frames.push(AggFrame::default());
        let r = self.bind_select_rest(sel, q, from, filter, input_width);
        self.frames.pop();
        r
    }

    fn bind_select_rest(
        &mut self,
        sel: &a::Select,
        q: &a::Query,
        from: From,
        filter: Option<Expr>,
        input_width: usize,
    ) -> PgResult<(Query, Vec<OutCol>)> {
        // GROUP BY (bound in the input scope, before aggregate extraction).
        let mut group_keys: Vec<Expr> = vec![];
        let mut group_asts: Vec<a::Expr> = vec![];
        let group_exprs = match &sel.group_by {
            a::GroupByExpr::Expressions(e, m) => {
                if !m.is_empty() {
                    return Err(unsupported("GROUP BY modifiers"));
                }
                e.clone()
            }
            a::GroupByExpr::All(_) => return Err(unsupported("GROUP BY ALL")),
        };
        // Select list is bound first so GROUP BY can refer to output names.
        let (mut proj, mut out_cols, mut proj_asts) = self.bind_projection(&sel.projection)?;
        // GROUPING SETS / ROLLUP / CUBE: each set as indexes into the group
        // keys. Plain GROUP BY expressions are in every set; several set
        // clauses combine as a cross product, as in Postgres.
        let mut sets: Option<Vec<Vec<usize>>> = None;
        let mut plain: Vec<usize> = vec![];
        let key_of = |this: &mut Self, keys: &mut Vec<Expr>, e: &a::Expr| -> PgResult<usize> {
            let te = this.bind_expr_no_agg(e, "GROUP BY")?;
            Ok(match keys.iter().position(|k| *k == te.e) {
                Some(i) => i,
                None => {
                    keys.push(te.e);
                    keys.len() - 1
                }
            })
        };
        for g in &group_exprs {
            let lists: Option<Vec<Vec<a::Expr>>> = match g {
                a::Expr::Rollup(items) => {
                    let mut out = vec![];
                    for n in (0..=items.len()).rev() {
                        out.push(items[..n].concat());
                    }
                    Some(out)
                }
                a::Expr::Cube(items) => {
                    let n = items.len();
                    let mut out = vec![];
                    for mask in (0..(1u32 << n)).rev() {
                        let mut set = vec![];
                        for (i, it) in items.iter().enumerate() {
                            if mask & (1 << (n - 1 - i)) != 0 {
                                set.extend(it.iter().cloned());
                            }
                        }
                        out.push(set);
                    }
                    Some(out)
                }
                a::Expr::GroupingSets(items) => Some(items.clone()),
                _ => None,
            };
            if let Some(lists) = lists {
                let mut these = vec![];
                for l in lists {
                    let mut idx = vec![];
                    for e in &l {
                        idx.push(key_of(self, &mut group_keys, e)?);
                    }
                    these.push(idx);
                }
                sets = Some(match sets {
                    None => these,
                    Some(prev) => prev
                        .iter()
                        .flat_map(|p| these.iter().map(move |t| [p.clone(), t.clone()].concat()))
                        .collect(),
                });
                continue;
            }
            let before = group_keys.len();
            // GROUP BY 1 / GROUP BY alias
            if let a::Expr::Value(v) = g
                && let a::Value::Number(n, _) = &v.value
                && let Ok(idx) = n.parse::<usize>()
            {
                if idx < 1 || idx > proj.len() {
                    return Err(PgError::new(
                        code::INVALID_COLUMN_REFERENCE,
                        format!("GROUP BY position {idx} is not in select list"),
                    ));
                }
                group_keys.push(proj[idx - 1].e.clone());
                group_asts.push(proj_asts[idx - 1].clone());
                continue;
            }
            if let a::Expr::Identifier(id) = g {
                let n = ident(id);
                if self.lookup_column(&n, None).is_none()
                    && let Some(p) = out_cols.iter().position(|c| c.name == n)
                {
                    group_keys.push(proj[p].e.clone());
                    group_asts.push(proj_asts[p].clone());
                    continue;
                }
            }
            let te = self.bind_expr_no_agg(g, "GROUP BY")?;
            group_keys.push(te.e);
            group_asts.push(g.clone());
            plain.extend(before..group_keys.len());
        }
        let grouping_sets = sets.map(|ss| {
            ss.into_iter()
                .map(|mut s| {
                    s.extend(plain.iter().copied());
                    s.sort_unstable();
                    s.dedup();
                    s
                })
                .collect::<Vec<_>>()
        });
        // HAVING
        let having_te = match &sel.having {
            Some(h) => Some(self.bind_expr(h)?),
            None => None,
        };
        // ORDER BY
        let mut order_specs: Vec<(TE, bool, bool, Option<usize>)> = vec![];
        if let Some(ob) = &q.order_by {
            let a::OrderByKind::Expressions(exprs) = &ob.kind else {
                return Err(unsupported("ORDER BY ALL"));
            };
            for o in exprs {
                let desc = o.options.sort == Some(a::OrderBySort::Desc);
                let nulls_first = o.options.nulls_first.unwrap_or(desc);
                // Ordinal or output-column name first.
                if let a::Expr::Value(v) = &o.expr
                    && let a::Value::Number(n, _) = &v.value
                    && let Ok(idx) = n.parse::<usize>()
                {
                    if idx < 1 || idx > out_cols.len() {
                        return Err(PgError::new(
                            code::INVALID_COLUMN_REFERENCE,
                            format!("ORDER BY position {idx} is not in select list"),
                        ));
                    }
                    order_specs.push((proj[idx - 1].clone(), desc, nulls_first, Some(idx - 1)));
                    continue;
                }
                // A simple name that matches an output column is always
                // taken as that output column, even when it also matches
                // (unambiguously or not) an input column: real Postgres
                // resolves ORDER BY names against the select list first.
                if let a::Expr::Identifier(id) = &o.expr {
                    let n = ident(id);
                    let hits: Vec<usize> = out_cols
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.name == n)
                        .map(|(i, _)| i)
                        .collect();
                    if hits.len() > 1 {
                        return Err(PgError::new(
                            code::AMBIGUOUS_COLUMN,
                            format!("ORDER BY \"{n}\" is ambiguous"),
                        ));
                    }
                    if let Some(&p) = hits.first() {
                        order_specs.push((proj[p].clone(), desc, nulls_first, Some(p)));
                        continue;
                    }
                }
                let te = self.bind_expr(&o.expr)?;
                let existing = proj.iter().position(|p| p.e == te.e);
                order_specs.push((te, desc, nulls_first, existing));
            }
        }
        // DISTINCT
        let mut distinct = Distinct::None;
        let mut distinct_on: Vec<TE> = vec![];
        // Output positions DISTINCT ON names by ordinal or output name,
        // resolved like ORDER BY's.
        let mut distinct_pos: Vec<Option<usize>> = vec![];
        match &sel.distinct {
            None | Some(a::Distinct::All) => {}
            Some(a::Distinct::Distinct) => distinct = Distinct::All,
            Some(a::Distinct::On(exprs)) => {
                for e in exprs {
                    if let a::Expr::Value(v) = e
                        && let a::Value::Number(n, _) = &v.value
                        && let Ok(idx) = n.parse::<usize>()
                    {
                        if idx < 1 || idx > out_cols.len() {
                            return Err(PgError::new(
                                code::INVALID_COLUMN_REFERENCE,
                                format!("DISTINCT ON position {idx} is not in select list"),
                            ));
                        }
                        distinct_on.push(proj[idx - 1].clone());
                        distinct_pos.push(Some(idx - 1));
                        continue;
                    }
                    if let a::Expr::Identifier(id) = e {
                        let n = ident(id);
                        let hits: Vec<usize> = out_cols
                            .iter()
                            .enumerate()
                            .filter(|(_, c)| c.name == n)
                            .map(|(i, _)| i)
                            .collect();
                        if hits.len() > 1 {
                            return Err(PgError::new(
                                code::AMBIGUOUS_COLUMN,
                                format!("DISTINCT ON \"{n}\" is ambiguous"),
                            ));
                        }
                        if let Some(&p) = hits.first() {
                            distinct_on.push(proj[p].clone());
                            distinct_pos.push(Some(p));
                            continue;
                        }
                    }
                    distinct_on.push(self.bind_expr(e)?);
                    distinct_pos.push(None);
                }
                distinct = Distinct::On(vec![]);
            }
        }
        let frame = self.frames.last_mut().unwrap();
        let aggs = std::mem::take(&mut frame.aggs);
        let mut windows = std::mem::take(&mut frame.wins);
        let grouped = !group_keys.is_empty() || !aggs.is_empty();
        // Rewrite everything above the aggregation step.
        let mut having = having_te.map(|te| te.e);
        if grouped {
            // Functional dependencies on a primary key don't apply across
            // grouping sets (a key may be NULLed out).
            if grouping_sets.is_none() {
                self.expand_group_keys(&mut group_keys);
            }
            for p in &mut proj {
                p.e = self.regroup(p.e.clone(), &group_keys, aggs.len())?;
            }
            if let Some(h) = having.take() {
                having = Some(self.regroup(h, &group_keys, aggs.len())?);
            }
            for (te, ..) in &mut order_specs {
                te.e = self.regroup(te.e.clone(), &group_keys, aggs.len())?;
            }
            for te in &mut distinct_on {
                te.e = self.regroup(te.e.clone(), &group_keys, aggs.len())?;
            }
            // Windows run over the grouped rows too.
            for w in &mut windows {
                for e in w.args.iter_mut().chain(w.partition.iter_mut()) {
                    *e = self.regroup(e.clone(), &group_keys, aggs.len())?;
                }
                for (e, ..) in &mut w.order {
                    *e = self.regroup(e.clone(), &group_keys, aggs.len())?;
                }
                if let Some(agg) = &mut w.agg {
                    for e in &mut agg.args {
                        *e = self.regroup(e.clone(), &group_keys, aggs.len())?;
                    }
                    if let Some(f) = &mut agg.filter {
                        *f = self.regroup(f.clone(), &group_keys, aggs.len())?;
                    }
                }
            }
        }
        if let Some(h) = &having {
            let _ = h;
        }
        let having = match having {
            Some(h) => Some(bool_check(h, "HAVING")?),
            None => None,
        };
        // Window function outputs sit right after the input/grouped row.
        let base_width = if grouped { group_keys.len() + aggs.len() } else { input_width };
        if !windows.is_empty() {
            for p in &mut proj {
                shift_winrefs(&mut p.e, base_width);
            }
            for (te, ..) in &mut order_specs {
                shift_winrefs(&mut te.e, base_width);
            }
            for te in &mut distinct_on {
                shift_winrefs(&mut te.e, base_width);
            }
        }
        // Projection, then hidden sort/distinct columns.
        let visible = proj.len();
        let mut exprs: Vec<Expr> = proj.iter().map(|p| p.e.clone()).collect();
        let mut order = vec![];
        for (te, desc, nulls_first, existing) in order_specs {
            // Enums sort by declaration order: sort on a hidden column of
            // their sort positions.
            if let (Base::Enum(_), false) = (te.ty.base, te.ty.array) {
                exprs.push(enum_order(te.e, te.ty));
                order.push(SortKey { col: exprs.len() - 1, desc, nulls_first });
                continue;
            }
            let col = match existing {
                Some(i) => i,
                None => {
                    // Plain DISTINCT only: DISTINCT ON may sort by anything
                    // (`DISTINCT ON (region) ... ORDER BY region, amt DESC`
                    // picks each region's largest row).
                    if distinct == Distinct::All {
                        return Err(PgError::new(
                            code::INVALID_COLUMN_REFERENCE,
                            "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                        ));
                    }
                    match exprs.iter().position(|e| *e == te.e) {
                        Some(i) => i,
                        None => {
                            exprs.push(te.e);
                            exprs.len() - 1
                        }
                    }
                }
            };
            order.push(SortKey { col, desc, nulls_first });
        }
        if let Distinct::On(_) = distinct {
            let mut idxs = vec![];
            for (te, pos) in distinct_on.into_iter().zip(distinct_pos) {
                let i = match pos.or_else(|| exprs.iter().position(|e| *e == te.e)) {
                    Some(i) => i,
                    None => {
                        exprs.push(te.e);
                        exprs.len() - 1
                    }
                };
                idxs.push(i);
            }
            // DISTINCT ON keys must be the leading ORDER BY keys.
            for (k, idx) in idxs.iter().enumerate() {
                if let Some(o) = order.get(k)
                    && o.col != *idx
                {
                    return Err(PgError::new(
                        code::INVALID_COLUMN_REFERENCE,
                        "SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
                    ));
                }
            }
            distinct = Distinct::On(idxs);
        }
        let (limit, offset) = self.bind_limit(q)?;
        let srf = exprs.iter().any(expr_has_srf);
        for c in out_cols.iter_mut() {
            if c.name.is_empty() {
                c.name = "?column?".into();
            }
        }
        proj_asts.clear();
        let select = Select {
            from,
            filter,
            group: grouped.then(|| group_keys.clone()),
            grouping_sets,
            aggs,
            having,
            windows,
            proj: exprs,
            visible,
            distinct,
            order,
            limit,
            offset,
            with_ties: q.fetch.as_ref().is_some_and(|f| f.with_ties),
            srf,
        };
        Ok((Query::Select(Box::new(select)), out_cols))
    }

    /// Adds columns functionally dependent on grouped primary keys.
    fn expand_group_keys(&self, keys: &mut Vec<Expr>) -> usize {
        let Some(scope) = self.scopes.last() else { return 0 };
        let before = keys.len();
        for rel in &scope.rels {
            let cols: Vec<&SCol> =
                scope.cols.iter().filter(|c| c.rel.as_deref() == Some(rel.as_str())).collect();
            let Some(oid) = cols.first().map(|c| c.table_oid).filter(|&o| o != 0) else {
                continue;
            };
            let Some(pk) = self.db.table(oid).and_then(|t| t.primary_key()) else { continue };
            if pk.cols.is_empty() {
                continue;
            }
            // Every primary-key column must be a group key itself.
            let covered = pk.cols.iter().all(|&k| {
                cols.iter()
                    .find(|c| c.attnum as usize == k + 1)
                    .is_some_and(|c| keys[..before].contains(&Expr::Col(c.idx)))
            });
            if !covered {
                continue;
            }
            for c in cols {
                let e = Expr::Col(c.idx);
                if !keys.contains(&e) {
                    keys.push(e);
                }
            }
        }
        keys.len() - before
    }

    fn bind_projection(
        &mut self,
        items: &[a::SelectItem],
    ) -> PgResult<(Vec<TE>, Vec<OutCol>, Vec<a::Expr>)> {
        let mut proj = vec![];
        let mut cols = vec![];
        let mut asts = vec![];
        for item in items {
            match item {
                a::SelectItem::UnnamedExpr(e) => {
                    let te = self.bind_expr(e)?;
                    let name = self.column_name(e, &te);
                    cols.push(OutCol {
                        name,
                        ty: te.ty,
                        typmod: te.typmod,
                        table_oid: te.table_oid,
                        attnum: te.attnum,
                        rec: te.rec.clone(),
                    });
                    proj.push(te);
                    asts.push(e.clone());
                }
                a::SelectItem::ExprWithAlias { expr, alias } => {
                    let te = self.bind_expr(expr)?;
                    cols.push(OutCol {
                        name: ident(alias),
                        ty: te.ty,
                        typmod: te.typmod,
                        table_oid: 0,
                        attnum: 0,
                        rec: te.rec.clone(),
                    });
                    proj.push(te);
                    asts.push(expr.clone());
                }
                a::SelectItem::Wildcard(_) => {
                    let scope = self.scopes.last().cloned().unwrap_or_default();
                    if scope.cols.is_empty() && scope.merged.is_empty() {
                        return Err(syntax("SELECT * with no tables specified is not valid"));
                    }
                    for m in &scope.merged {
                        cols.push(OutCol {
                            name: m.name.clone(),
                            ty: m.ty,
                            typmod: m.typmod,
                            table_oid: 0,
                            attnum: 0,
                            rec: None,
                        });
                        proj.push(TE {
                            e: m.expr.clone(),
                            ty: m.ty,
                            typmod: m.typmod,
                            table_oid: 0,
                            attnum: 0,
                            rec: None,
                            collation: None,
                        });
                        asts.push(a::Expr::Identifier(a::Ident::new(m.name.clone())));
                    }
                    for c in scope.cols.iter().filter(|c| !c.hidden) {
                        cols.push(OutCol {
                            name: c.name.clone(),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
                            rec: None,
                        });
                        proj.push(TE {
                            e: Expr::Col(c.idx),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
                            rec: None,
                            collation: None,
                        });
                        asts.push(a::Expr::Identifier(a::Ident::new(c.name.clone())));
                    }
                }
                a::SelectItem::QualifiedWildcard(kind, _) => {
                    let rel = match kind {
                        a::SelectItemQualifiedWildcardKind::ObjectName(n) => {
                            object_name(n).pop().unwrap_or_default()
                        }
                        a::SelectItemQualifiedWildcardKind::Expr(_) => {
                            return Err(unsupported("expression.*"));
                        }
                    };
                    let scope = self.scopes.last().cloned().unwrap_or_default();
                    if !scope.rels.contains(&rel) {
                        return Err(missing_from(&rel));
                    }
                    for c in scope
                        .cols
                        .iter()
                        .filter(|c| c.rel.as_deref() == Some(rel.as_str()) && !c.hidden)
                    {
                        cols.push(OutCol {
                            name: c.name.clone(),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
                            rec: None,
                        });
                        proj.push(TE {
                            e: Expr::Col(c.idx),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
                            rec: None,
                            collation: None,
                        });
                        asts.push(a::Expr::Identifier(a::Ident::new(c.name.clone())));
                    }
                }
                a::SelectItem::ExprWithAliases { .. } => {
                    return Err(unsupported("multiple column aliases"));
                }
            }
        }
        Ok((proj, cols, asts))
    }

    /// Replaces input-row references with grouped-row references.
    fn regroup(&self, e: Expr, keys: &[Expr], nagg: usize) -> PgResult<Expr> {
        let _ = nagg;
        if let Some(i) = keys.iter().position(|k| *k == e) {
            return Ok(Expr::Col(i));
        }
        match e {
            Expr::AggRef(i) => Ok(Expr::Col(keys.len() + i)),
            Expr::Col(i) => {
                let name = self
                    .scopes
                    .last()
                    .and_then(|s| s.cols.iter().find(|c| c.idx == i))
                    .map(|c| match &c.rel {
                        Some(r) => format!("{r}.{}", c.name),
                        None => c.name.clone(),
                    })
                    .unwrap_or_else(|| "?".into());
                Err(PgError::new(
                    code::GROUPING_ERROR,
                    format!(
                        "column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function"
                    ),
                ))
            }
            // A correlated subquery reads the grouped row: its references to
            // this query's columns become references to their group keys.
            Expr::Sub { kind, mut query } => {
                self.regroup_outer(&mut query, keys)?;
                Ok(Expr::Sub { kind, query })
            }
            Expr::InSub { left, op, mut query, all, negated } => {
                let left = left
                    .into_iter()
                    .map(|l| self.regroup(l, keys, nagg))
                    .collect::<PgResult<Vec<_>>>()?;
                self.regroup_outer(&mut query, keys)?;
                Ok(Expr::InSub { left, op, query, all, negated })
            }
            mut other => {
                let mut err = None;
                other.children_mut(&mut |c| {
                    if err.is_some() {
                        return;
                    }
                    match self.regroup(c.clone(), keys, nagg) {
                        Ok(n) => *c = n,
                        Err(e) => err = Some(e),
                    }
                });
                match err {
                    Some(e) => Err(e),
                    None => Ok(other),
                }
            }
        }
    }

    /// Points a subquery's references to this (grouped) query's input
    /// columns at the group keys; an ungrouped one is Postgres's 42803.
    fn regroup_outer(&self, q: &mut Query, keys: &[Expr]) -> PgResult<()> {
        let mut err = None;
        map_query_exprs(q, 0, &mut |e, level| {
            if err.is_some() {
                return;
            }
            if let Expr::Outer(d, i) = e
                && *d == level + 1
            {
                match keys.iter().position(|k| *k == Expr::Col(*i)) {
                    Some(p) => *i = p,
                    None => {
                        let i = *i;
                        let name = self
                            .scopes
                            .last()
                            .and_then(|s| s.cols.iter().find(|c| c.idx == i))
                            .map(|c| match &c.rel {
                                Some(r) => format!("{r}.{}", c.name),
                                None => c.name.clone(),
                            })
                            .unwrap_or_else(|| "?".into());
                        err = Some(PgError::new(
                            code::GROUPING_ERROR,
                            format!("subquery uses ungrouped column \"{name}\" from outer query"),
                        ));
                    }
                }
            }
        });
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // -----------------------------------------------------------------
    // FROM

    fn bind_from(&mut self, items: &[a::TableWithJoins]) -> PgResult<(From, Scope)> {
        if items.is_empty() {
            return Ok((From::One, Scope::default()));
        }
        let (mut from, mut scope) = self.bind_table_with_joins(&items[0])?;
        for item in &items[1..] {
            let (rhs, rscope) = self.bind_table_with_joins_at(item, scope.width(), Some(&scope))?;
            let left_cols = scope.width();
            let right_cols = rscope.width();
            merge_scopes(&mut scope, rscope)?;
            from = From::Join {
                left: Box::new(from),
                right: Box::new(rhs),
                kind: JoinKind::Cross,
                on: None,
                lateral: is_lateral_item(&item.relation),
                left_cols,
                right_cols,
            };
        }
        Ok((from, scope))
    }

    fn bind_table_with_joins(&mut self, t: &a::TableWithJoins) -> PgResult<(From, Scope)> {
        self.bind_table_with_joins_at(t, 0, None)
    }

    /// `outer` is the FROM items to the left of a comma, which a function or
    /// LATERAL item may refer to.
    fn bind_table_with_joins_at(
        &mut self,
        t: &a::TableWithJoins,
        base: usize,
        outer: Option<&Scope>,
    ) -> PgResult<(From, Scope)> {
        let (mut from, mut scope) = self.bind_factor(&t.relation, base, outer)?;
        for join in &t.joins {
            let left_cols = scope.width();
            let (rhs, rscope) = self.bind_factor(&join.relation, base + left_cols, Some(&scope))?;
            let right_cols = rscope.width();
            let (kind, constraint) = join_kind(&join.join_operator)?;
            let mut combined = scope.clone();
            merge_scopes(&mut combined, rscope.clone())?;
            let mut on = None;
            match constraint {
                Some(a::JoinConstraint::On(e)) => {
                    self.scopes.push(combined.clone());
                    self.frames.push(AggFrame { forbid: Some("JOIN/ON"), ..Default::default() });
                    let te = self.bind_expr(&e);
                    self.frames.pop();
                    self.scopes.pop();
                    on = Some(self.bool_expr(te?, "JOIN/ON")?);
                }
                Some(a::JoinConstraint::Using(names)) => {
                    let cols: Vec<String> =
                        names.iter().map(|n| object_name(n).pop().unwrap_or_default()).collect();
                    on = Some(self.using_join(&mut combined, &scope, &rscope, &cols, &kind)?);
                }
                Some(a::JoinConstraint::Natural) => {
                    let mut cols = vec![];
                    for l in scope.cols.iter().filter(|c| !c.hidden) {
                        if rscope.cols.iter().any(|r| !r.hidden && r.name == l.name)
                            && !cols.contains(&l.name)
                        {
                            cols.push(l.name.clone());
                        }
                    }
                    on = if cols.is_empty() {
                        None
                    } else {
                        Some(self.using_join(&mut combined, &scope, &rscope, &cols, &kind)?)
                    };
                }
                Some(a::JoinConstraint::None) | None => {}
            }
            // Columns are numbered across the whole FROM list, but the join
            // sees only its own item's rows.
            if base > 0
                && let Some(e) = on.as_mut()
            {
                rebase_cols(e, base);
            }
            scope = combined;
            from = From::Join {
                left: Box::new(from),
                right: Box::new(rhs),
                kind,
                on,
                lateral: is_lateral_item(&join.relation),
                left_cols,
                right_cols,
            };
        }
        Ok((from, scope))
    }

    fn using_join(
        &mut self,
        combined: &mut Scope,
        left: &Scope,
        right: &Scope,
        names: &[String],
        kind: &JoinKind,
    ) -> PgResult<Expr> {
        let mut conds = vec![];
        for name in names {
            let l = left.cols.iter().filter(|c| !c.hidden && c.name == *name).collect::<Vec<_>>();
            let r = right.cols.iter().filter(|c| !c.hidden && c.name == *name).collect::<Vec<_>>();
            if l.is_empty() || r.is_empty() {
                return Err(PgError::new(
                    code::UNDEFINED_COLUMN,
                    format!(
                        "column \"{name}\" specified in USING clause does not exist in {} table",
                        if l.is_empty() { "left" } else { "right" }
                    ),
                ));
            }
            if l.len() > 1 || r.len() > 1 {
                return Err(PgError::new(
                    code::AMBIGUOUS_COLUMN,
                    format!("common column name \"{name}\" appears more than once in left table"),
                ));
            }
            let (l, r) = (l[0].clone(), r[0].clone());
            let ty = self.common_type(&[l.ty, r.ty], "JOIN/USING", 0)?;
            let le = self.cast_to(Expr::Col(l.idx), l.ty, ty)?;
            let re = self.cast_to(Expr::Col(r.idx), r.ty, ty)?;
            conds.push(Expr::Compare {
                op: CmpOp::Eq,
                left: Box::new(le.clone()),
                right: Box::new(re.clone()),
                bpchar: ty.base == Base::Bpchar,
            });
            let expr = match kind {
                JoinKind::Full => Expr::Coalesce(vec![le, re]),
                JoinKind::Right => re,
                _ => le,
            };
            combined.merged.push(Merged {
                name: name.clone(),
                expr,
                ty,
                typmod: if l.typmod == r.typmod { l.typmod } else { -1 },
            });
            for c in combined.cols.iter_mut() {
                if c.name == *name && (c.idx == l.idx || c.idx == r.idx) {
                    c.hidden = true;
                }
            }
        }
        Ok(and_all(conds))
    }

    /// Binds one FROM item. `left` is the scope visible to LATERAL items.
    fn bind_factor(
        &mut self,
        f: &a::TableFactor,
        base: usize,
        left: Option<&Scope>,
    ) -> PgResult<(From, Scope)> {
        match f {
            a::TableFactor::Table { name, alias, args, with_ordinality, .. } => {
                let parts = object_name(name);
                if let Some(args) = args {
                    // Table function: implicitly LATERAL.
                    let fname = parts.last().cloned().unwrap_or_default();
                    let arg_exprs: Vec<a::Expr> = args
                        .args
                        .iter()
                        .map(|fa| match fa {
                            a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(e)) => Ok(e.clone()),
                            _ => Err(unsupported("named function arguments")),
                        })
                        .collect::<PgResult<_>>()?;
                    return self.bind_from_function(
                        &fname,
                        &arg_exprs,
                        alias.as_ref(),
                        base,
                        left,
                        *with_ordinality,
                    );
                }
                let (from, cols, default_alias) = self.resolve_relation(&parts)?;
                let mut scope = Scope::default();
                let rel = match alias {
                    Some(al) => ident(&al.name),
                    None => default_alias,
                };
                let mut cols = cols;
                if let Some(al) = alias {
                    apply_alias_columns(&mut cols, &al.columns, &rel)?;
                }
                for (i, c) in cols.iter().enumerate() {
                    scope.cols.push(SCol {
                        rel: Some(rel.clone()),
                        name: c.name.clone(),
                        ty: c.ty,
                        typmod: c.typmod,
                        idx: base + i,
                        table_oid: c.table_oid,
                        attnum: c.attnum,
                        hidden: false,
                        system: false,
                        rec: None,
                    });
                }
                if let From::Table { oid, .. } = &from {
                    push_system_cols(&mut scope, &rel, *oid, base + cols.len());
                }
                scope.rels.push(rel);
                Ok((from, scope))
            }
            a::TableFactor::Derived { lateral, subquery, alias, .. } => {
                let placeholder = match (*lateral, left) {
                    (true, Some(l)) => l.clone(),
                    _ => Scope::default(),
                };
                self.scopes.push(placeholder);
                let bound = self.bind_query(subquery);
                self.scopes.pop();
                let (mut query, mut cols) = bound?;
                resolve_unknown_output(&mut query, &mut cols);
                let rel = match alias {
                    Some(al) => ident(&al.name),
                    None => String::new(),
                };
                if let Some(al) = alias {
                    apply_alias_columns(&mut cols, &al.columns, &rel)?;
                }
                let mut scope = Scope::default();
                for (i, c) in cols.iter().enumerate() {
                    scope.cols.push(SCol {
                        rel: (!rel.is_empty()).then(|| rel.clone()),
                        name: c.name.clone(),
                        ty: c.ty,
                        typmod: c.typmod,
                        idx: base + i,
                        table_oid: 0,
                        attnum: 0,
                        hidden: false,
                        system: false,
                        rec: c.rec.clone(),
                    });
                }
                if !rel.is_empty() {
                    scope.rels.push(rel);
                }
                Ok((From::Sub(Box::new(query)), scope))
            }
            a::TableFactor::NestedJoin { table_with_joins, alias } => {
                if alias.is_some() {
                    return Err(unsupported("aliased join"));
                }
                self.bind_table_with_joins_at(table_with_joins, base, None)
            }
            a::TableFactor::Function { lateral: _, name, args, alias, with_ordinality } => {
                let fname = object_name(name).pop().unwrap_or_default();
                let arg_exprs: Vec<a::Expr> = args
                    .iter()
                    .map(|fa| match fa {
                        a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(e)) => Ok(e.clone()),
                        _ => Err(unsupported("named function arguments")),
                    })
                    .collect::<PgResult<_>>()?;
                self.bind_from_function(
                    &fname,
                    &arg_exprs,
                    alias.as_ref(),
                    base,
                    left,
                    *with_ordinality,
                )
            }
            a::TableFactor::UNNEST { alias, array_exprs, with_ordinality, .. } => self
                .bind_from_function(
                    "unnest",
                    array_exprs,
                    alias.as_ref(),
                    base,
                    left,
                    *with_ordinality,
                ),
            other => Err(unsupported(&format!("FROM item {}", first_words(&other.to_string())))),
        }
    }

    /// The user function `name` callable with `nargs` arguments.
    fn user_function(&self, name: &str, nargs: usize) -> Option<super::catalog::Function> {
        self.db
            .functions
            .values()
            .find(|f| {
                f.name == name
                    && !f.procedure
                    && nargs <= f.arg_types.len()
                    && nargs + f.arg_defaults.iter().filter(|d| d.is_some()).count()
                        >= f.arg_types.len()
            })
            .cloned()
    }

    fn bind_from_function(
        &mut self,
        fname: &str,
        args: &[a::Expr],
        alias: Option<&a::TableAlias>,
        base: usize,
        left: Option<&Scope>,
        ordinality: bool,
    ) -> PgResult<(From, Scope)> {
        self.scopes.push(left.cloned().unwrap_or_default());
        self.frames.push(AggFrame { forbid: Some("FROM"), ..Default::default() });
        let bound: PgResult<Vec<TE>> = args.iter().map(|e| self.bind_expr(e)).collect();
        self.frames.pop();
        self.scopes.pop();
        let mut bound = bound?;
        // A lateral function's arguments are evaluated against the left-hand
        // row, whose columns start at zero.
        if let Some(l) = left {
            let first = l.cols.iter().map(|c| c.idx).min().unwrap_or(0);
            if first > 0 {
                for t in &mut bound {
                    rebase_cols(&mut t.e, first);
                }
            }
        }
        let arg_tys: Vec<Type> = bound.iter().map(|t| t.ty).collect();
        // `unnest(a, b, ...)` in FROM: one column per array, padded with NULLs.
        let multi_unnest = fname == "unnest" && bound.len() > 1;
        let mut user = None;
        let (sig_name, arg_exprs, arg_tys, mut cols) = if multi_unnest {
            let mut cols = vec![];
            for t in &arg_tys {
                let elem = match t.base {
                    _ if t.array => t.elem(),
                    Base::Int2Vector => Type::INT2,
                    Base::OidVector => Type::OID,
                    _ => {
                        // Reports "function unnest(...) does not exist".
                        sigs::resolve("unnest", &[*t])?;
                        return Err(PgError::new(code::UNDEFINED_FUNCTION, "unnest needs arrays"));
                    }
                };
                cols.push(OutCol::new("unnest".to_string(), elem));
            }
            let exprs = bound.into_iter().map(|t| t.e).collect();
            ("unnest", exprs, arg_tys, cols)
        } else if sigs::kind_of(fname).is_none()
            && let Some(uf) = self.user_function(fname, bound.len())
        {
            let mut arg_exprs = vec![];
            for (i, te) in bound.into_iter().enumerate() {
                arg_exprs.push(self.coerce(te, uf.arg_types[i], -1, CastCtx::Implicit, fname)?);
            }
            let cols: Vec<OutCol> = if uf.out_cols.is_empty() {
                vec![OutCol::new(fname.to_string(), uf.ret)]
            } else {
                uf.out_cols.iter().map(|(n, t)| OutCol::new(n.clone(), *t)).collect()
            };
            user = Some(uf.oid);
            ("user_function", arg_exprs, uf.arg_types.clone(), cols)
        } else {
            let r = sigs::resolve(fname, &arg_tys)?;
            let mut arg_exprs = vec![];
            for (te, target) in bound.into_iter().zip(r.arg_tys.iter()) {
                arg_exprs.push(self.coerce(te, *target, -1, CastCtx::Implicit, fname)?);
            }
            let cols: Vec<OutCol> = if r.sig.cols.is_empty() {
                vec![OutCol::new(fname.to_string(), r.ret)]
            } else {
                r.sig.cols.iter().map(|(n, t)| OutCol::new(n.to_string(), *t)).collect()
            };
            (r.sig.name, arg_exprs, r.arg_tys, cols)
        };
        if ordinality {
            cols.push(OutCol::new("ordinality", Type::INT8));
        }
        let rel = match alias {
            Some(al) => ident(&al.name),
            None => fname.to_string(),
        };
        if let Some(al) = alias {
            if al.columns.is_empty() && cols.len() == 1 && !ordinality {
                // `f(...) AS x` names the single output column too.
                cols[0].name = rel.clone();
            }
            apply_alias_columns(&mut cols, &al.columns, &rel)?;
        }
        let mut scope = Scope::default();
        for (i, c) in cols.iter().enumerate() {
            scope.cols.push(SCol {
                rel: Some(rel.clone()),
                name: c.name.clone(),
                ty: c.ty,
                typmod: -1,
                idx: base + i,
                table_oid: 0,
                attnum: 0,
                hidden: false,
                system: false,
                rec: None,
            });
        }
        scope.rels.push(rel);
        let ncols = cols.len();
        Ok((
            From::Func {
                name: sig_name,
                args: arg_exprs,
                arg_tys,
                ncols,
                ordinality,
                lateral: left.is_some(),
                user,
            },
            scope,
        ))
    }

    /// Resolves a relation name to a FROM source and its columns.
    fn resolve_relation(&mut self, parts: &[String]) -> PgResult<(From, Vec<OutCol>, String)> {
        let full = parts.join(".");
        let name = parts.last().cloned().unwrap_or_default();
        let schema = if parts.len() > 1 { Some(parts[parts.len() - 2].clone()) } else { None };
        if parts.len() > 3 {
            return Err(PgError::new(
                code::SYNTAX_ERROR,
                format!("improper qualified name (too many dotted names): {full}"),
            ));
        }
        // CTEs (innermost first).
        if schema.is_none()
            && let Some(cte) = self.ctes.iter().rev().find(|c| c.name == name)
        {
            return Ok((From::Cte(cte.slot), cte.cols.clone(), name));
        }
        // System catalogs.
        if pgcatalog::is_catalog_relation(schema.as_deref(), &name) {
            let key = pgcatalog::relation_key(schema.as_deref(), &name);
            let cols = pgcatalog::columns(&key);
            return Ok((From::Virtual { name: key, ncols: cols.len() }, cols, name));
        }
        let oid = match self.lookup_table_oid(schema.as_deref(), &name) {
            Ok(oid) => oid,
            // A sequence reads as a one-row table of its state.
            Err(e) => {
                let Ok(seq) = self.lookup_sequence_oid(schema.as_deref(), &name) else {
                    return Err(e);
                };
                let cols = vec![
                    OutCol::new("last_value", Type::INT8),
                    OutCol::new("log_cnt", Type::INT8),
                    OutCol::new("is_called", Type::BOOL),
                ];
                let key = format!("{}{seq}", pgcatalog::SEQUENCE_STATE_PREFIX);
                return Ok((From::Virtual { name: key, ncols: 3 }, cols, name));
            }
        };
        let t = self.db.table(oid).unwrap();
        if t.kind == RelKind::View {
            if self.expanding_views.contains(&oid) {
                return Err(PgError::new(
                    code::INVALID_OBJECT_DEFINITION,
                    format!("infinite recursion detected in rules for relation \"{}\"", t.name),
                ));
            }
            let sql = t.view_sql.clone().unwrap_or_default();
            let stmts = super::parse_sql(&sql)?;
            let a::Statement::Query(q) = &stmts[0] else {
                return Err(PgError::new(code::INTERNAL_ERROR, "bad view definition"));
            };
            let saved = std::mem::take(&mut self.scopes);
            self.expanding_views.push(oid);
            let bound = self.bind_query(q);
            self.expanding_views.pop();
            self.scopes = saved;
            let (query, qcols) = bound?;
            let cols: Vec<OutCol> = t
                .live_columns()
                .zip(qcols.iter())
                .map(|((i, c), _qc)| OutCol {
                    name: c.name.clone(),
                    ty: c.ty,
                    typmod: c.typmod,
                    table_oid: oid,
                    attnum: i as i16 + 1,
                    rec: None,
                })
                .collect();
            return Ok((From::Sub(Box::new(query)), cols, name));
        }
        let cols = table_out_cols(t);
        Ok((From::Table { oid, ncols: t.live_columns().count() + SYSTEM_COLS.len() }, cols, name))
    }

    /// The oid of the sequence `schema.name` (or found on the search path).
    pub fn lookup_sequence_oid(&self, schema: Option<&str>, name: &str) -> PgResult<u32> {
        let full = match schema {
            Some(s) => format!("{s}.{name}"),
            None => name.to_string(),
        };
        let schemas: Vec<String> = match schema {
            Some(s) => vec![s.to_string()],
            None => self.sess.search_path.clone(),
        };
        schemas
            .iter()
            .filter_map(|s| self.db.schema_by_name(s))
            .find_map(|sid| self.db.find_sequence(sid, name).map(|q| q.oid))
            .ok_or_else(|| super::catalog::undefined_table(&full))
    }

    pub fn lookup_table_oid(&self, schema: Option<&str>, name: &str) -> PgResult<u32> {
        let full = match schema {
            Some(s) => format!("{s}.{name}"),
            None => name.to_string(),
        };
        match schema {
            // A missing schema reads as a missing relation, as in Postgres.
            Some(s) => self
                .db
                .schema_by_name(s)
                .and_then(|sid| self.db.find_table(sid, name))
                .map(|t| t.oid)
                .ok_or_else(|| super::catalog::undefined_table(&full)),
            None => {
                for s in &self.sess.search_path {
                    if let Some(sid) = self.db.schema_by_name(s)
                        && let Some(t) = self.db.find_table(sid, name)
                    {
                        return Ok(t.oid);
                    }
                }
                Err(super::catalog::undefined_table(&full))
            }
        }
    }

    // -----------------------------------------------------------------
    // Expressions

    fn lookup_column(&self, name: &str, rel: Option<&str>) -> Option<(usize, TE)> {
        for (depth, scope) in self.scopes.iter().rev().enumerate() {
            if rel.is_none()
                && let Some(m) = scope.merged.iter().find(|m| m.name == name)
            {
                let e = if depth == 0 { m.expr.clone() } else { outerize(m.expr.clone(), depth) };
                return Some((
                    depth,
                    TE {
                        e,
                        ty: m.ty,
                        typmod: m.typmod,
                        table_oid: 0,
                        attnum: 0,
                        rec: None,
                        collation: None,
                    },
                ));
            }
            let hits: Vec<&SCol> = scope
                .cols
                .iter()
                .filter(|c| {
                    c.name == name
                        && (rel.is_none() && (!c.hidden || c.system)
                            || rel.is_some_and(|r| c.rel.as_deref() == Some(r)))
                })
                .collect();
            if let Some(c) = hits.first() {
                let e = if depth == 0 { Expr::Col(c.idx) } else { Expr::Outer(depth, c.idx) };
                return Some((
                    depth,
                    TE {
                        e,
                        ty: c.ty,
                        typmod: c.typmod,
                        table_oid: c.table_oid,
                        attnum: c.attnum,
                        rec: c.rec.clone(),
                        collation: None,
                    },
                ));
            }
        }
        None
    }

    fn column_ambiguous(&self, name: &str) -> bool {
        for scope in self.scopes.iter().rev() {
            if scope.merged.iter().any(|m| m.name == name) {
                return false;
            }
            let n = scope.cols.iter().filter(|c| c.name == name && (!c.hidden || c.system)).count();
            if n > 0 {
                return n > 1;
            }
        }
        false
    }

    fn rel_in_scope(&self, rel: &str) -> bool {
        self.scopes.iter().any(|s| s.rels.iter().any(|r| r == rel))
    }

    fn bind_column(&mut self, name: &str, rel: Option<&str>) -> PgResult<TE> {
        if rel.is_none() && self.column_ambiguous(name) {
            return Err(PgError::new(
                code::AMBIGUOUS_COLUMN,
                format!("column reference \"{name}\" is ambiguous"),
            ));
        }
        if let Some((_, te)) = self.lookup_column(name, rel) {
            return Ok(te);
        }
        // SQL keywords that are functions without parentheses.
        if rel.is_none()
            && matches!(
                name,
                "current_schema"
                    | "current_catalog"
                    | "current_role"
                    | "current_user"
                    | "session_user"
                    | "user"
            )
        {
            return self.bind_keyword_function(name);
        }
        match rel {
            Some(r) if !self.rel_in_scope(r) => Err(missing_from(r)),
            Some(r) => Err(PgError::new(
                code::UNDEFINED_COLUMN,
                format!("column {r}.{name} does not exist"),
            )),
            None => Err(PgError::new(
                code::UNDEFINED_COLUMN,
                format!("column \"{name}\" does not exist"),
            )),
        }
    }

    fn bind_expr_no_agg(&mut self, e: &a::Expr, what: &'static str) -> PgResult<TE> {
        self.frames.push(AggFrame { forbid: Some(what), ..Default::default() });
        let r = self.bind_expr(e);
        self.frames.pop();
        r
    }

    fn bool_expr(&mut self, te: TE, what: &str) -> PgResult<Expr> {
        let e = self.coerce(te, Type::BOOL, -1, CastCtx::Implicit, what)?;
        Ok(e)
    }

    pub fn bind_expr(&mut self, e: &a::Expr) -> PgResult<TE> {
        use a::Expr as E;
        match e {
            E::Identifier(id) => {
                let n = ident(id);
                // A bare table name is a whole-row reference.
                if self.lookup_column(&n, None).is_none() && self.rel_in_scope(&n) {
                    return self.whole_row(&n);
                }
                self.bind_column(&n, None)
            }
            E::CompoundIdentifier(ids) => {
                let parts: Vec<String> = ids.iter().map(ident).collect();
                match parts.len() {
                    2 => self.bind_column(&parts[1], Some(&parts[0])),
                    3 => self.bind_column(&parts[2], Some(&parts[1])),
                    _ => Err(PgError::new(
                        code::UNDEFINED_COLUMN,
                        format!("column {} does not exist", parts.join(".")),
                    )),
                }
            }
            E::Nested(inner) => self.bind_expr(inner),
            E::Value(v) => self.bind_literal(&v.value),
            E::TypedString(ts) => {
                let (ty, typmod) = self.data_type(&ts.data_type)?;
                let (a::Value::SingleQuotedString(s) | a::Value::EscapedStringLiteral(s)) =
                    &ts.value.value
                else {
                    return Err(syntax("invalid typed literal"));
                };
                let v = types::from_text(s, ty, &self.dctx())?;
                let v = types::apply_typmod(v, ty, typmod, true)?;
                Ok(TE::new(Expr::Const(v), ty).with_typmod(typmod))
            }
            E::Interval(iv) => {
                let te = self.bind_expr(&iv.value)?;
                let text = match &te.e {
                    Expr::Const(Value::Text(s)) => s.clone(),
                    _ => {
                        let e = self.coerce(te, Type::TEXT, -1, CastCtx::Explicit, "interval")?;
                        return Ok(TE::new(
                            Expr::Cast {
                                expr: Box::new(e),
                                from: Type::TEXT,
                                to: Type::INTERVAL,
                                typmod: -1,
                                explicit: true,
                            },
                            Type::INTERVAL,
                        ));
                    }
                };
                // INTERVAL '1' DAY: the unit qualifies a bare number.
                let full = match (&iv.leading_field, &iv.last_field) {
                    (Some(f), _) if !text.chars().any(|c| c.is_ascii_alphabetic()) => {
                        format!("{text} {}", field_unit(f))
                    }
                    _ => text.clone(),
                };
                let v = types::from_text(&full, Type::INTERVAL, &self.dctx())?;
                Ok(TE::new(Expr::Const(v), Type::INTERVAL))
            }
            E::Cast { kind, expr, data_type, format } => {
                if format.is_some() {
                    return Err(unsupported("CAST ... FORMAT"));
                }
                if matches!(kind, a::CastKind::TryCast | a::CastKind::SafeCast) {
                    return Err(unsupported("TRY_CAST"));
                }
                let (ty, typmod) = self.data_type(data_type)?;
                let te = self.bind_expr(expr)?;
                let from = te.ty;
                let e = self.coerce(te, ty, typmod, CastCtx::Explicit, "cast")?;
                let _ = from;
                Ok(TE::new(e, ty).with_typmod(typmod))
            }
            E::UnaryOp { op, expr } => self.bind_unary(op, expr),
            E::BinaryOp { left, op, right } => self.bind_binary(left, op, right),
            E::IsNull(x) => {
                let te = self.bind_expr(x)?;
                Ok(TE::new(Expr::IsNull(Box::new(te.e), false), Type::BOOL))
            }
            E::IsNotNull(x) => {
                let te = self.bind_expr(x)?;
                Ok(TE::new(Expr::IsNull(Box::new(te.e), true), Type::BOOL))
            }
            E::IsTrue(x)
            | E::IsNotTrue(x)
            | E::IsFalse(x)
            | E::IsNotFalse(x)
            | E::IsUnknown(x)
            | E::IsNotUnknown(x) => {
                let (want, negated) = match e {
                    E::IsTrue(_) => (Some(true), false),
                    E::IsNotTrue(_) => (Some(true), true),
                    E::IsFalse(_) => (Some(false), false),
                    E::IsNotFalse(_) => (Some(false), true),
                    E::IsUnknown(_) => (None, false),
                    _ => (None, true),
                };
                let te = self.bind_expr(x)?;
                let inner = self.coerce(te, Type::BOOL, -1, CastCtx::Implicit, "IS")?;
                Ok(TE::new(Expr::IsBool(Box::new(inner), want, negated), Type::BOOL))
            }
            E::IsDistinctFrom(l, r) | E::IsNotDistinctFrom(l, r) => {
                let negated = matches!(e, E::IsNotDistinctFrom(..));
                let lt = self.bind_expr(l)?;
                let rt = self.bind_expr(r)?;
                let ty = self.common_type(&[lt.ty, rt.ty], "IS DISTINCT FROM", 0)?;
                let le = self.coerce(lt, ty, -1, CastCtx::Implicit, "IS DISTINCT FROM")?;
                let re = self.coerce(rt, ty, -1, CastCtx::Implicit, "IS DISTINCT FROM")?;
                Ok(TE::new(
                    Expr::Distinct { left: Box::new(le), right: Box::new(re), negated },
                    Type::BOOL,
                ))
            }
            E::Between { expr, negated, low, high } => {
                let x = self.bind_expr(expr)?;
                let lo = self.bind_expr(low)?;
                let hi = self.bind_expr(high)?;
                let ge = self.compare(x.clone(), lo, CmpOp::Ge)?;
                let le = self.compare(x, hi, CmpOp::Le)?;
                let both = Expr::And(vec![ge, le]);
                Ok(TE::new(if *negated { Expr::Not(Box::new(both)) } else { both }, Type::BOOL))
            }
            E::InList { expr, list, negated } => {
                let x = self.bind_expr(expr)?;
                let mut items = vec![];
                let mut tys = vec![x.ty];
                for i in list {
                    let te = self.bind_expr(i)?;
                    tys.push(te.ty);
                    items.push(te);
                }
                let ty = self.common_type(&tys, "IN", 0)?;
                let ci = ty.is_string()
                    && (self.case_insensitive(&x)
                        || items.iter().any(|i| self.case_insensitive(i)));
                let fold = |e: Expr| if ci { fold_case(e) } else { e };
                let xe = fold(self.coerce(x, ty, -1, CastCtx::Implicit, "IN")?);
                let mut list_e = vec![];
                for it in items {
                    list_e.push(fold(self.coerce(it, ty, -1, CastCtx::Implicit, "IN")?));
                }
                Ok(TE::new(
                    Expr::InList { expr: Box::new(xe), list: list_e, negated: *negated },
                    Type::BOOL,
                ))
            }
            E::InSubquery { expr, subquery, negated } => {
                let lefts = match expr.as_ref() {
                    E::Tuple(items) => {
                        items.iter().map(|i| self.bind_expr(i)).collect::<PgResult<Vec<_>>>()?
                    }
                    other => vec![self.bind_expr(other)?],
                };
                let (q, cols) = self.bind_query(subquery)?;
                if cols.len() != lefts.len() {
                    return Err(PgError::new(code::SYNTAX_ERROR, "subquery has too many columns"));
                }
                let mut left_e = vec![];
                for (te, c) in lefts.into_iter().zip(&cols) {
                    let ty = self.common_type(&[te.ty, c.ty], "IN", 0)?;
                    left_e.push(self.coerce(te, ty, -1, CastCtx::Implicit, "IN")?);
                }
                Ok(TE::new(
                    Expr::InSub {
                        left: left_e,
                        op: CmpOp::Eq,
                        query: Box::new(q),
                        all: *negated,
                        negated: *negated,
                    },
                    Type::BOOL,
                ))
            }
            E::Exists { subquery, negated } => {
                let (q, _) = self.bind_query(subquery)?;
                let e = Expr::Sub { kind: SubKind::Exists, query: Box::new(q) };
                Ok(TE::new(if *negated { Expr::Not(Box::new(e)) } else { e }, Type::BOOL))
            }
            E::Subquery(q) => {
                let (mut plan, mut cols) = self.bind_query(q)?;
                resolve_unknown_output(&mut plan, &mut cols);
                if cols.len() != 1 {
                    return Err(PgError::new(
                        code::SYNTAX_ERROR,
                        "subquery must return only one column",
                    ));
                }
                Ok(TE {
                    e: Expr::Sub { kind: SubKind::Scalar, query: Box::new(plan) },
                    ty: cols[0].ty,
                    typmod: cols[0].typmod,
                    table_oid: 0,
                    attnum: 0,
                    rec: cols[0].rec.clone(),
                    collation: None,
                })
            }
            E::AnyOp { left, compare_op, right, .. } | E::AllOp { left, compare_op, right } => {
                let all = matches!(e, E::AllOp { .. });
                let op = cmp_op(compare_op)
                    .ok_or_else(|| unsupported(&format!("operator {compare_op} with ANY/ALL")))?;
                let lt = self.bind_expr(left)?;
                if let E::Subquery(q) = strip_nested(right) {
                    let (plan, cols) = self.bind_query(q)?;
                    let ty = self.common_type(&[lt.ty, cols[0].ty], "ANY", 0)?;
                    let le = self.coerce(lt, ty, -1, CastCtx::Implicit, "ANY")?;
                    return Ok(TE::new(
                        Expr::InSub {
                            left: vec![le],
                            op,
                            query: Box::new(plan),
                            all,
                            negated: false,
                        },
                        Type::BOOL,
                    ));
                }
                let rt = self.bind_expr(right)?;
                let elem = match rt.ty.base {
                    Base::Int2Vector if !rt.ty.array => Type::INT2,
                    Base::OidVector if !rt.ty.array => Type::OID,
                    _ if rt.ty.array => rt.ty.elem(),
                    _ => rt.ty,
                };
                let ty = self.common_type(&[lt.ty, elem], "ANY", 0)?;
                let le = self.coerce(lt, ty, -1, CastCtx::Implicit, "ANY")?;
                let re = if matches!(rt.ty.base, Base::Int2Vector | Base::OidVector) {
                    rt.e
                } else {
                    self.coerce(rt, ty.to_array(), -1, CastCtx::Implicit, "ANY")?
                };
                Ok(TE::new(
                    Expr::AnyAll { left: Box::new(le), op, right: Box::new(re), all },
                    Type::BOOL,
                ))
            }
            E::Case { operand, conditions, else_result, .. } => {
                self.bind_case(operand, conditions, else_result)
            }
            E::Function(f) => self.bind_function(f),
            E::Extract { field, expr, .. } => {
                let unit = field_unit(field);
                let te = self.bind_expr(expr)?;
                self.call("extract", vec![TE::new(Expr::Const(Value::text(unit)), Type::TEXT), te])
            }
            E::Position { expr, r#in } => {
                let sub = self.bind_expr(expr)?;
                let s = self.bind_expr(r#in)?;
                self.call("position", vec![s, sub])
            }
            E::Substring { expr, substring_from, substring_for, shorthand, .. } => {
                let fname = if *shorthand { "substr" } else { "substring" };
                let mut args = vec![self.bind_expr(expr)?];
                if let Some(f) = substring_from {
                    args.push(self.bind_expr(f)?);
                }
                if let Some(l) = substring_for {
                    args.push(self.bind_expr(l)?);
                }
                self.call(fname, args)
            }
            E::Trim { trim_where, trim_what, expr, trim_characters } => {
                let name = match trim_where {
                    Some(a::TrimWhereField::Leading) => "ltrim",
                    Some(a::TrimWhereField::Trailing) => "rtrim",
                    _ => "btrim",
                };
                let mut args = vec![self.bind_expr(expr)?];
                if let Some(w) = trim_what {
                    args.push(self.bind_expr(w)?);
                } else if let Some(cs) = trim_characters
                    && let Some(c) = cs.first()
                {
                    args.push(self.bind_expr(c)?);
                }
                self.call(name, args)
            }
            E::Overlay { expr, overlay_what, overlay_from, overlay_for } => {
                let s = self.bind_expr(expr)?;
                let what = self.bind_expr(overlay_what)?;
                let from = self.bind_expr(overlay_from)?;
                let len = match overlay_for {
                    Some(l) => self.bind_expr(l)?,
                    None => self.call("length", vec![what.clone()])?,
                };
                let one = TE::new(Expr::Const(Value::Int(1)), Type::INT4);
                let head_len = self.sub_one(from.clone())?;
                let head = self.call("substr", vec![s.clone(), one, head_len])?;
                let tail_start = self.binop_te("+", from, len)?;
                let tail = self.call("substr", vec![s, tail_start])?;
                let cat = self.binop_te("||", head, what)?;
                self.binop_te("||", cat, tail)
            }
            E::Ceil { expr, field } => {
                let te = self.bind_expr(expr)?;
                match field {
                    a::CeilFloorKind::DateTimeField(a::DateTimeField::NoDateTime) => {
                        self.call("ceil", vec![te])
                    }
                    _ => Err(unsupported("CEIL ... TO")),
                }
            }
            E::Floor { expr, field } => {
                let te = self.bind_expr(expr)?;
                match field {
                    a::CeilFloorKind::DateTimeField(a::DateTimeField::NoDateTime) => {
                        self.call("floor", vec![te])
                    }
                    _ => Err(unsupported("FLOOR ... TO")),
                }
            }
            E::Like { negated, expr, pattern, escape_char, any }
            | E::ILike { negated, expr, pattern, escape_char, any } => {
                if *any {
                    return Err(unsupported("LIKE ANY"));
                }
                let ci = matches!(e, E::ILike { .. });
                let s = self.bind_expr(expr)?;
                let p = self.bind_expr(pattern)?;
                if [&s, &p].iter().any(|t| self.collation_of(t).is_some_and(|i| !i.deterministic)) {
                    return Err(PgError::new(
                        code::FEATURE_NOT_SUPPORTED,
                        format!(
                            "nondeterministic collations are not supported for {}",
                            if ci { "ILIKE" } else { "LIKE" }
                        ),
                    ));
                }
                let op = match (ci, negated) {
                    (false, false) => "~~",
                    (false, true) => "!~~",
                    (true, false) => "~~*",
                    (true, true) => "!~~*",
                };
                if let Some(esc) = escape_char {
                    let e = self.bind_expr(esc)?;
                    let s = self.coerce(s, Type::TEXT, -1, CastCtx::Implicit, "LIKE")?;
                    let p = self.coerce(p, Type::TEXT, -1, CastCtx::Implicit, "LIKE")?;
                    let esc = self.coerce(e, Type::TEXT, -1, CastCtx::Implicit, "LIKE")?;
                    let name = if ci { "like_escape_ci" } else { "like_escape" };
                    let call = Expr::Call {
                        name,
                        args: vec![s, p, esc],
                        ty: Type::BOOL,
                        arg_tys: vec![Type::TEXT, Type::TEXT, Type::TEXT],
                    };
                    return Ok(TE::new(
                        if *negated { Expr::Not(Box::new(call)) } else { call },
                        Type::BOOL,
                    ));
                }
                self.binop_te(op, s, p)
            }
            E::SimilarTo { negated, expr, pattern, escape_char } => {
                let s = self.bind_expr(expr)?;
                let p = self.bind_expr(pattern)?;
                let s = self.coerce(s, Type::TEXT, -1, CastCtx::Implicit, "SIMILAR TO")?;
                let p = self.coerce(p, Type::TEXT, -1, CastCtx::Implicit, "SIMILAR TO")?;
                let esc = match escape_char {
                    Some(e) => {
                        let te = self.bind_expr(e)?;
                        self.coerce(te, Type::TEXT, -1, CastCtx::Implicit, "SIMILAR TO")?
                    }
                    None => Expr::Const(Value::text("\\")),
                };
                let call = Expr::Call {
                    name: "similar_to",
                    args: vec![s, p, esc],
                    ty: Type::BOOL,
                    arg_tys: vec![Type::TEXT, Type::TEXT, Type::TEXT],
                };
                Ok(TE::new(if *negated { Expr::Not(Box::new(call)) } else { call }, Type::BOOL))
            }
            E::Array(arr) => {
                let mut items = vec![];
                let mut tys = vec![];
                for el in &arr.elem {
                    let te = self.bind_expr(el)?;
                    tys.push(te.ty);
                    items.push(te);
                }
                if items.is_empty() {
                    return Ok(TE::new(Expr::Array(vec![]), Type::array_of(Base::Text)));
                }
                let elem_ty = self.common_type(&tys, "ARRAY", 0)?;
                let inner_array = elem_ty.array;
                let mut out = vec![];
                for it in items {
                    out.push(self.coerce(it, elem_ty, -1, CastCtx::Implicit, "ARRAY")?);
                }
                let ty = if inner_array { elem_ty } else { elem_ty.to_array() };
                Ok(TE::new(Expr::Array(out), ty))
            }
            E::Tuple(items) => {
                let mut out = vec![];
                for i in items {
                    out.push(self.bind_expr(i)?.e);
                }
                Ok(TE::new(Expr::Row(out), Type::RECORD))
            }
            E::CompoundFieldAccess { root, access_chain } => self.bind_access(root, access_chain),
            E::AtTimeZone { timestamp, time_zone } => {
                let ts = self.bind_expr(timestamp)?;
                let tz = self.bind_expr(time_zone)?;
                self.call("timezone", vec![tz, ts])
            }
            E::Collate { expr, collation } => {
                let mut te = self.bind_expr(expr)?;
                let name = name_parts(collation).pop().unwrap_or_default();
                if !super::collation::collatable(te.ty) && !te.ty.is_unknown() {
                    return Err(PgError::new(
                        code::DATATYPE_MISMATCH,
                        format!("collations are not supported by type {}", te.ty.display(-1)),
                    ));
                }
                super::collation::resolve(self.db, &name)?;
                te.collation = Some(name);
                Ok(te)
            }
            E::Prefixed { prefix, value } => {
                // e.g. B'1010' / X'ff' style prefixed literals.
                let _ = prefix;
                self.bind_expr(value)
            }
            E::JsonAccess { .. } => Err(unsupported("JSON path access")),
            other => Err(unsupported(&format!("expression {}", first_words(&other.to_string())))),
        }
    }

    fn sub_one(&mut self, te: TE) -> PgResult<TE> {
        self.binop_te("-", te, TE::new(Expr::Const(Value::Int(1)), Type::INT4))
    }

    fn dctx(&self) -> super::datetime::Ctx<'_> {
        super::datetime::Ctx { now: self.sess.now, zone: &self.sess.fmt.zone }
    }

    fn whole_row(&mut self, rel: &str) -> PgResult<TE> {
        let scope = self.scopes.last().cloned().unwrap_or_default();
        let cols: Vec<&SCol> =
            scope.cols.iter().filter(|c| c.rel.as_deref() == Some(rel) && !c.hidden).collect();
        if cols.is_empty() {
            return Err(missing_from(rel));
        }
        let exprs = cols.iter().map(|c| Expr::Col(c.idx)).collect();
        Ok(TE::new(Expr::Row(exprs), Type::RECORD))
    }

    fn bind_literal(&mut self, v: &a::Value) -> PgResult<TE> {
        Ok(match v {
            a::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i32>() {
                    TE::new(Expr::Const(Value::Int(i as i64)), Type::INT4)
                } else if !n.contains(['.', 'e', 'E'])
                    && let Ok(i) = n.parse::<i64>()
                {
                    TE::new(Expr::Const(Value::Int(i)), Type::INT8)
                } else {
                    let num = types::parse_numeric(n)?;
                    TE::new(Expr::Const(Value::Num(num)), Type::NUMERIC)
                }
            }
            a::Value::SingleQuotedString(s)
            | a::Value::DoubleQuotedString(s)
            | a::Value::TripleSingleQuotedString(s)
            | a::Value::TripleDoubleQuotedString(s) => {
                TE::new(Expr::Const(Value::text(s.clone())), Type::UNKNOWN)
            }
            // sqlparser has already decoded the backslash escapes.
            a::Value::EscapedStringLiteral(s) => {
                TE::new(Expr::Const(Value::text(s.clone())), Type::UNKNOWN)
            }
            a::Value::DollarQuotedString(s) => {
                TE::new(Expr::Const(Value::text(s.value.clone())), Type::UNKNOWN)
            }
            a::Value::UnicodeStringLiteral(s) => {
                TE::new(Expr::Const(Value::text(s.clone())), Type::UNKNOWN)
            }
            a::Value::NationalStringLiteral(s) => {
                TE::new(Expr::Const(Value::text(s.clone())), Type::UNKNOWN)
            }
            a::Value::HexStringLiteral(s) => {
                let v = i64::from_str_radix(s, 16).map_err(|_| syntax("invalid hex literal"))?;
                TE::new(Expr::Const(Value::Int(v)), Type::INT8)
            }
            a::Value::Boolean(b) => TE::new(Expr::Const(Value::Bool(*b)), Type::BOOL),
            a::Value::Null => TE::new(Expr::Const(Value::Null), Type::UNKNOWN),
            a::Value::Placeholder(p) => {
                let idx: usize = p
                    .strip_prefix('$')
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| syntax(format!("there is no parameter {p}")))?;
                if idx == 0 {
                    return Err(syntax("there is no parameter $0"));
                }
                if self.params.len() < idx {
                    self.params.resize(idx, Type::UNKNOWN);
                }
                TE::new(Expr::Param(idx - 1), self.params[idx - 1])
            }
            other => return Err(unsupported(&format!("literal {other}"))),
        })
    }

    fn bind_case(
        &mut self,
        operand: &Option<Box<a::Expr>>,
        conditions: &[a::CaseWhen],
        else_result: &Option<Box<a::Expr>>,
    ) -> PgResult<TE> {
        let op = match operand {
            Some(o) => Some(self.bind_expr(o)?),
            None => None,
        };
        let mut whens = vec![];
        let mut result_tys = vec![];
        for w in conditions {
            let cond = self.bind_expr(&w.condition)?;
            let res = self.bind_expr(&w.result)?;
            result_tys.push(res.ty);
            whens.push((cond, res));
        }
        let else_ = match else_result {
            Some(e) => {
                let te = self.bind_expr(e)?;
                result_tys.push(te.ty);
                Some(te)
            }
            None => None,
        };
        let ty = self.common_type(&result_tys, "CASE", 0)?;
        let mut out_whens = vec![];
        for (cond, res) in whens {
            let c = match &op {
                Some(o) => self.compare(o.clone(), cond, CmpOp::Eq)?,
                None => self.bool_expr(cond, "CASE")?,
            };
            let r = self.coerce(res, ty, -1, CastCtx::Implicit, "CASE")?;
            out_whens.push((c, r));
        }
        let else_e = match else_ {
            Some(te) => Some(Box::new(self.coerce(te, ty, -1, CastCtx::Implicit, "CASE")?)),
            None => None,
        };
        let case = Expr::Case { operand: None, whens: out_whens, else_: else_e };
        if expr_has_srf(&case) {
            return Err(PgError::new(
                code::FEATURE_NOT_SUPPORTED,
                "set-returning functions are not allowed in CASE",
            ));
        }
        Ok(TE::new(case, ty))
    }

    fn bind_access(&mut self, root: &a::Expr, chain: &[a::AccessExpr]) -> PgResult<TE> {
        // `t.col[1]` parses as a Dot on the relation name: resolve the column
        // first, then apply the rest of the chain.
        let mut chain = chain;
        let mut te = match (root, chain.first()) {
            (a::Expr::Identifier(rel), Some(a::AccessExpr::Dot(a::Expr::Identifier(col))))
                if self.lookup_column(&ident(rel), None).is_none()
                    && self.rel_in_scope(&ident(rel)) =>
            {
                let te = self.bind_column(&ident(col), Some(&ident(rel)))?;
                chain = &chain[1..];
                te
            }
            _ => self.bind_expr(root)?,
        };
        for acc in chain {
            match acc {
                a::AccessExpr::Subscript(a::Subscript::Index { index }) => {
                    let idx = self.bind_expr(index)?;
                    // jsonb takes a key (text) or an index (integer).
                    let json = matches!(te.ty.base, Base::Jsonb | Base::Json) && !te.ty.array;
                    let key_ty =
                        if json && !matches!(idx.ty.base, Base::Int2 | Base::Int4 | Base::Int8) {
                            Type::TEXT
                        } else {
                            Type::INT4
                        };
                    let idx = self.coerce(idx, key_ty, -1, CastCtx::Assignment, "subscript")?;
                    let vector_elem = casts::vector_as_array(te.ty).map(|a| a.elem());
                    if !te.ty.array
                        && vector_elem.is_none()
                        && te.ty.base != Base::Jsonb
                        && te.ty.base != Base::Json
                    {
                        return Err(PgError::new(
                            code::DATATYPE_MISMATCH,
                            format!(
                                "cannot subscript type {} because it does not support subscripting",
                                te.ty.display(-1)
                            ),
                        ));
                    }
                    let ret = match vector_elem {
                        Some(e) => e,
                        None if te.ty.array => te.ty.elem(),
                        None => te.ty,
                    };
                    te = TE::new(
                        Expr::Call {
                            name: "subscript",
                            args: vec![te.e, idx],
                            ty: ret,
                            arg_tys: vec![te.ty, key_ty],
                        },
                        ret,
                    );
                }
                a::AccessExpr::Subscript(a::Subscript::Slice {
                    lower_bound,
                    upper_bound,
                    stride,
                }) => {
                    if stride.is_some() {
                        return Err(unsupported("array slice stride"));
                    }
                    let lo = match lower_bound {
                        Some(e) => {
                            let t = self.bind_expr(e)?;
                            self.coerce(t, Type::INT4, -1, CastCtx::Assignment, "subscript")?
                        }
                        None => Expr::Const(Value::Null),
                    };
                    let hi = match upper_bound {
                        Some(e) => {
                            let t = self.bind_expr(e)?;
                            self.coerce(t, Type::INT4, -1, CastCtx::Assignment, "subscript")?
                        }
                        None => Expr::Const(Value::Null),
                    };
                    let ty = te.ty;
                    te = TE::new(
                        Expr::Call {
                            name: "slice",
                            args: vec![te.e, lo, hi],
                            ty,
                            arg_tys: vec![ty, Type::INT4, Type::INT4],
                        },
                        ty,
                    );
                }
                a::AccessExpr::Dot(field) => {
                    let a::Expr::Identifier(id) = field else {
                        return Err(unsupported("field selection from a composite value"));
                    };
                    let fname = ident(id);
                    let fields = te
                        .rec
                        .clone()
                        .ok_or_else(|| unsupported("field selection from a composite value"))?;
                    let idx = fields.iter().position(|(n, _)| *n == fname).ok_or_else(|| {
                        PgError::new(
                            code::UNDEFINED_COLUMN,
                            format!("column \"{fname}\" not found in data type record"),
                        )
                    })?;
                    let ty = fields[idx].1;
                    te = TE::new(
                        Expr::Call {
                            name: "record_field",
                            args: vec![te.e, Expr::Const(Value::Int(idx as i64))],
                            ty,
                            arg_tys: vec![Type::RECORD, Type::INT4],
                        },
                        ty,
                    );
                }
            }
        }
        Ok(te)
    }

    fn bind_unary(&mut self, op: &a::UnaryOperator, expr: &a::Expr) -> PgResult<TE> {
        use a::UnaryOperator as U;
        let te = self.bind_expr(expr)?;
        match op {
            U::Not => {
                let e = self.bool_expr(te, "NOT")?;
                Ok(TE::new(Expr::Not(Box::new(e)), Type::BOOL))
            }
            U::Minus | U::Plus => {
                let ty = if te.ty.is_unknown() { Type::NUMERIC } else { te.ty };
                if !ty.is_numeric() && ty.base != Base::Interval {
                    return Err(PgError::new(
                        code::UNDEFINED_FUNCTION,
                        format!("operator does not exist: {} {}", if matches!(op, U::Minus) { "-" } else { "+" }, ty.display(-1)),
                    )
                    .hint("No operator matches the given name and argument types. You might need to add explicit type casts."));
                }
                let e = self.coerce(te, ty, -1, CastCtx::Implicit, "-")?;
                if matches!(op, U::Plus) {
                    return Ok(TE::new(e, ty));
                }
                // Fold negated literals so -2147483648 stays int4.
                if let Expr::Const(v) = &e
                    && let Ok(neg) = super::funcs::unop("-", v, ty)
                {
                    return Ok(TE::new(Expr::Const(neg), ty));
                }
                Ok(TE::new(Expr::Call { name: "-u", args: vec![e], ty, arg_tys: vec![ty] }, ty))
            }
            U::BitwiseNot => {
                let ty = if te.ty.is_unknown() { Type::INT4 } else { te.ty };
                let e = self.coerce(te, ty, -1, CastCtx::Implicit, "~")?;
                Ok(TE::new(Expr::Call { name: "~u", args: vec![e], ty, arg_tys: vec![ty] }, ty))
            }
            U::PGSquareRoot | U::PGCubeRoot => {
                let e = self.coerce(te, Type::FLOAT8, -1, CastCtx::Implicit, "|/")?;
                let name = if matches!(op, U::PGSquareRoot) { "|/u" } else { "||/u" };
                Ok(TE::new(
                    Expr::Call {
                        name,
                        args: vec![e],
                        ty: Type::FLOAT8,
                        arg_tys: vec![Type::FLOAT8],
                    },
                    Type::FLOAT8,
                ))
            }
            U::PGAbs => {
                let ty = if te.ty.is_unknown() { Type::NUMERIC } else { te.ty };
                let e = self.coerce(te, ty, -1, CastCtx::Implicit, "@")?;
                Ok(TE::new(Expr::Call { name: "@u", args: vec![e], ty, arg_tys: vec![ty] }, ty))
            }
            U::PGPostfixFactorial | U::PGPrefixFactorial => {
                let e = self.coerce(te, Type::INT8, -1, CastCtx::Implicit, "!")?;
                Ok(TE::new(
                    Expr::Call {
                        name: "factorial",
                        args: vec![e],
                        ty: Type::NUMERIC,
                        arg_tys: vec![Type::INT8],
                    },
                    Type::NUMERIC,
                ))
            }
            other => Err(unsupported(&format!("unary operator {other}"))),
        }
    }

    fn compare(&mut self, l: TE, r: TE, op: CmpOp) -> PgResult<Expr> {
        // Row comparisons compare field by field.
        if let (Expr::Row(le), Expr::Row(re)) = (&l.e, &r.e) {
            if le.len() != re.len() {
                return Err(PgError::new(
                    code::SYNTAX_ERROR,
                    "unequal number of entries in row expressions",
                ));
            }
            let mut conds = vec![];
            for (a, b) in le.iter().zip(re) {
                conds.push(Expr::Compare {
                    op,
                    left: Box::new(a.clone()),
                    right: Box::new(b.clone()),
                    bpchar: false,
                });
            }
            return Ok(and_all(conds));
        }
        // `xid = int4` (Postgres's xideqint4): `RETURNING xmax = 0`.
        let int_like = |t: Type| matches!(t.base, Base::Int2 | Base::Int4 | Base::Int8) && !t.array;
        let as_int8 = |te: TE| TE::new(te.e, Type::INT8);
        let (l, r) = match (l.ty.base, r.ty.base) {
            (Base::Xid, _) if int_like(r.ty) && matches!(op, CmpOp::Eq | CmpOp::Ne) => {
                (as_int8(l), r)
            }
            (_, Base::Xid) if int_like(l.ty) && matches!(op, CmpOp::Eq | CmpOp::Ne) => {
                (l, as_int8(r))
            }
            _ => (l, r),
        };
        let ty = self
            .common_type(&[l.ty, r.ty], "comparison", 0)
            .map_err(|_| no_operator(op.symbol(), l.ty, r.ty))?;
        // A case-insensitive (nondeterministic) collation on either side
        // compares the case-folded values.
        let ci = ty.is_string() && (self.case_insensitive(&l) || self.case_insensitive(&r));
        let le = self.coerce(l, ty, -1, CastCtx::Implicit, "comparison")?;
        let re = self.coerce(r, ty, -1, CastCtx::Implicit, "comparison")?;
        let (le, re) = if ci { (fold_case(le), fold_case(re)) } else { (le, re) };
        // Enums compare by declaration order, not by label.
        let (le, re) = (enum_order(le, ty), enum_order(re, ty));
        Ok(Expr::Compare {
            op,
            left: Box::new(le),
            right: Box::new(re),
            bpchar: ty.base == Base::Bpchar,
        })
    }

    /// The collation an operand compares under: an explicit COLLATE, else
    /// its column's.
    fn collation_of(&self, te: &TE) -> Option<super::collation::Info> {
        let name = te.collation.clone().or_else(|| {
            (te.attnum > 0)
                .then(|| self.db.table(te.table_oid))
                .flatten()
                .and_then(|t| t.columns.get(te.attnum as usize - 1))
                .and_then(|c| c.collation.clone())
        })?;
        super::collation::lookup(self.db, &name)
    }

    fn case_insensitive(&self, te: &TE) -> bool {
        self.collation_of(te).is_some_and(|i| super::collation::case_insensitive(&i))
    }

    /// `(s1, e1) OVERLAPS (s2, e2)`: each end may be an interval (a
    /// length from the start); periods are normalized so start <= end and
    /// overlap when they share an instant (Postgres's definition, with an
    /// equal start always overlapping).
    fn bind_overlaps(&mut self, left: &a::Expr, right: &a::Expr) -> PgResult<TE> {
        let pair = |e: &a::Expr| -> PgResult<(a::Expr, a::Expr)> {
            match e {
                a::Expr::Tuple(items) if items.len() == 2 => {
                    Ok((items[0].clone(), items[1].clone()))
                }
                a::Expr::Nested(inner) => match inner.as_ref() {
                    a::Expr::Tuple(items) if items.len() == 2 => {
                        Ok((items[0].clone(), items[1].clone()))
                    }
                    _ => Err(PgError::new(
                        code::SYNTAX_ERROR,
                        "wrong number of parameters on left side of OVERLAPS expression",
                    )),
                },
                _ => Err(PgError::new(
                    code::SYNTAX_ERROR,
                    "wrong number of parameters on left side of OVERLAPS expression",
                )),
            }
        };
        let ((s1, e1), (s2, e2)) = (pair(left)?, pair(right)?);
        let mut ends = vec![];
        for (s, e) in [(s1, e1), (s2, e2)] {
            let st = self.bind_expr(&s)?;
            let en = self.bind_expr(&e)?;
            // An interval end is start + interval.
            let en = if en.ty.base == Base::Interval && !en.ty.array {
                self.bind_binary(&s, &a::BinaryOperator::Plus, &e)?
            } else {
                en
            };
            ends.push((st, en));
        }
        let tys: Vec<Type> = ends.iter().flat_map(|(a, b)| [a.ty, b.ty]).collect();
        let ty = self.common_type(&tys, "OVERLAPS", 0)?;
        let mut ex = vec![];
        for (st, en) in ends {
            let st = self.coerce(st, ty, -1, CastCtx::Implicit, "OVERLAPS")?;
            let en = self.coerce(en, ty, -1, CastCtx::Implicit, "OVERLAPS")?;
            // NULL in, NULL out (least/greatest would skip it).
            let any_null = Expr::Or(vec![
                Expr::IsNull(Box::new(st.clone()), false),
                Expr::IsNull(Box::new(en.clone()), false),
            ]);
            let guard = |e: Expr| Expr::Case {
                operand: None,
                whens: vec![(any_null.clone(), Expr::Const(Value::Null))],
                else_: Some(Box::new(e)),
            };
            ex.push((
                guard(Expr::Greatest(vec![st.clone(), en.clone()], true)),
                guard(Expr::Greatest(vec![st, en], false)),
            ));
        }
        let ((a1, b1), (a2, b2)) = (ex[0].clone(), ex[1].clone());
        let cmp = |op: CmpOp, l: &Expr, r: &Expr| Expr::Compare {
            op,
            left: Box::new(l.clone()),
            right: Box::new(r.clone()),
            bpchar: false,
        };
        let e = Expr::Or(vec![
            Expr::And(vec![cmp(CmpOp::Gt, &a1, &a2), cmp(CmpOp::Lt, &a1, &b2)]),
            Expr::And(vec![cmp(CmpOp::Gt, &a2, &a1), cmp(CmpOp::Lt, &a2, &b1)]),
            cmp(CmpOp::Eq, &a1, &a2),
        ]);
        Ok(TE::new(e, Type::BOOL))
    }

    fn bind_binary(
        &mut self,
        left: &a::Expr,
        op: &a::BinaryOperator,
        right: &a::Expr,
    ) -> PgResult<TE> {
        use a::BinaryOperator as B;
        if matches!(op, B::Overlaps) {
            return self.bind_overlaps(left, right);
        }
        if matches!(op, B::And | B::Or) {
            let l = self.bind_expr(left)?;
            let r = self.bind_expr(right)?;
            let what = if matches!(op, B::And) { "AND" } else { "OR" };
            let le = self.bool_expr(l, what)?;
            let re = self.bool_expr(r, what)?;
            let e =
                if matches!(op, B::And) { Expr::And(vec![le, re]) } else { Expr::Or(vec![le, re]) };
            return Ok(TE::new(e, Type::BOOL));
        }
        let l = self.bind_expr(left)?;
        let r = self.bind_expr(right)?;
        if let Some(c) = cmp_op(op) {
            return Ok(TE::new(self.compare(l, r, c)?, Type::BOOL));
        }
        let name = match op {
            B::Plus => "+",
            B::Minus => "-",
            B::Multiply => "*",
            B::Divide => "/",
            B::Modulo => "%",
            B::PGExp => "^",
            B::StringConcat => "||",
            B::BitwiseAnd => "&",
            B::BitwiseOr => "|",
            B::PGBitwiseXor | B::BitwiseXor => "#",
            B::PGBitwiseShiftLeft => "<<",
            B::PGBitwiseShiftRight => ">>",
            B::Arrow => "->",
            B::LongArrow => "->>",
            B::HashArrow => "#>",
            B::HashLongArrow => "#>>",
            B::AtArrow => "@>",
            B::ArrowAt => "<@",
            B::AtAt => "@@",
            B::AtQuestion => "@?",
            B::Question => "?",
            B::QuestionPipe => "?|",
            B::QuestionAnd => "?&",
            B::HashMinus => "#-",
            B::PGOverlap => "&&",
            B::PGRegexMatch => "~",
            B::PGRegexIMatch => "~*",
            B::PGRegexNotMatch => "!~",
            B::PGRegexNotIMatch => "!~*",
            B::PGLikeMatch => "~~",
            B::PGILikeMatch => "~~*",
            B::PGNotLikeMatch => "!~~",
            B::PGNotILikeMatch => "!~~*",
            B::PGStartsWith => "^@",
            // `x OPERATOR(pg_catalog.~) y` names an operator by schema.
            B::PGCustomBinaryOperator(parts) => match parts.last().map(String::as_str) {
                Some("=") => return Ok(TE::new(self.compare(l, r, CmpOp::Eq)?, Type::BOOL)),
                Some("<>") | Some("!=") => {
                    return Ok(TE::new(self.compare(l, r, CmpOp::Ne)?, Type::BOOL));
                }
                Some("<") => return Ok(TE::new(self.compare(l, r, CmpOp::Lt)?, Type::BOOL)),
                Some("<=") => return Ok(TE::new(self.compare(l, r, CmpOp::Le)?, Type::BOOL)),
                Some(">") => return Ok(TE::new(self.compare(l, r, CmpOp::Gt)?, Type::BOOL)),
                Some(">=") => return Ok(TE::new(self.compare(l, r, CmpOp::Ge)?, Type::BOOL)),
                Some(op) => match known_operator(op) {
                    Some(name) => name,
                    None => return Err(unsupported(&format!("operator {op}"))),
                },
                None => return Err(unsupported("operator")),
            },
            other => return Err(unsupported(&format!("operator {other}"))),
        };
        self.binop_te(name, l, r)
    }

    /// Resolves an operator's operand and result types.
    fn binop_te(&mut self, op: &'static str, l: TE, r: TE) -> PgResult<TE> {
        let (lt, rt) = (l.ty, r.ty);
        let both_unknown = lt.is_unknown() && rt.is_unknown();
        let is_inet = |t: Type| !t.array && matches!(t.base, Base::Inet | Base::Cidr);
        let inet_op = is_inet(lt) || is_inet(rt);
        let (ltarget, rtarget, ret): (Type, Type, Type) = match op {
            // inet: containment, address arithmetic.
            "<<" | ">>" | "<<=" | ">>=" | "&&" if inet_op => {
                (Type::of(Base::Inet), Type::of(Base::Inet), Type::BOOL)
            }
            "-" if inet_op && (is_inet(rt) || (rt.is_unknown() && is_inet(lt))) => {
                (Type::of(Base::Inet), Type::of(Base::Inet), Type::INT8)
            }
            "+" | "-" if is_inet(lt) => (Type::of(Base::Inet), Type::INT8, Type::of(Base::Inet)),
            "+" if is_inet(rt) => (Type::INT8, Type::of(Base::Inet), Type::of(Base::Inet)),
            "&" | "|" if inet_op => {
                (Type::of(Base::Inet), Type::of(Base::Inet), Type::of(Base::Inet))
            }
            "+" | "-" | "*" | "/" | "%" | "^" => {
                if both_unknown {
                    return Err(PgError::new(
                        code::AMBIGUOUS_FUNCTION,
                        format!("operator is not unique: unknown {op} unknown"),
                    )
                    .hint("Could not choose a best candidate operator. You might need to add explicit type casts."));
                }
                // date + unknown (int or interval?) and timetz + unknown
                // have no best candidate in Postgres.
                let known = if lt.is_unknown() { rt } else { lt };
                if op == "+"
                    && (lt.is_unknown() || rt.is_unknown())
                    && !known.array
                    && matches!(known.base, Base::Date | Base::Timetz)
                {
                    let (l, r) = if lt.is_unknown() {
                        ("unknown".to_string(), rt.display(-1))
                    } else {
                        (lt.display(-1), "unknown".to_string())
                    };
                    return Err(PgError::new(
                        code::AMBIGUOUS_FUNCTION,
                        format!("operator is not unique: {l} + {r}"),
                    )
                    .hint("Could not choose a best candidate operator. You might need to add explicit type casts."));
                }
                let lt = if lt.is_unknown() { guess_unknown(rt, op) } else { lt };
                let rt = if rt.is_unknown() { guess_unknown(lt, op) } else { rt };
                match self.arith_types(op, lt, rt) {
                    Some(t) => t,
                    None => return Err(no_operator(op, lt, rt)),
                }
            }
            "||" => {
                if lt.base == Base::Tsvector && rt.base == Base::Tsvector {
                    (Type::TSVECTOR, Type::TSVECTOR, Type::TSVECTOR)
                } else if lt.array || rt.array {
                    let elem = if lt.array { lt.elem() } else { lt };
                    let relem = if rt.array { rt.elem() } else { rt };
                    let e = self
                        .common_type(&[elem, relem], "||", 0)
                        .map_err(|_| no_operator(op, lt, rt))?;
                    let l2 = if lt.array { e.to_array() } else { e };
                    let r2 = if rt.array { e.to_array() } else { e };
                    (l2, r2, e.to_array())
                } else if lt.base == Base::Jsonb || rt.base == Base::Jsonb {
                    (Type::JSONB, Type::JSONB, Type::JSONB)
                } else if lt.base == Base::Bytea && rt.base == Base::Bytea {
                    (Type::BYTEA, Type::BYTEA, Type::BYTEA)
                } else {
                    let l2 = if lt.is_unknown() { Type::TEXT } else { lt };
                    let r2 = if rt.is_unknown() { Type::TEXT } else { rt };
                    if !l2.is_string() && !r2.is_string() && !both_unknown {
                        return Err(no_operator(op, lt, rt));
                    }
                    (l2, r2, Type::TEXT)
                }
            }
            "&" | "|" | "#" | "<<" | ">>" => {
                let width = |t: Type| match t.base {
                    Base::Int2 => 2,
                    Base::Int4 => 4,
                    _ => 8,
                };
                let shift = matches!(op, "<<" | ">>");
                let t = if lt.is_unknown() && rt.is_unknown() {
                    Type::INT4
                } else if lt.is_integer() && (shift || rt.is_unknown() || !rt.is_integer()) {
                    lt
                } else if lt.is_integer() && rt.is_integer() {
                    // int2 & int4 runs as int4 & int4, the wider operand.
                    if width(rt) > width(lt) { rt } else { lt }
                } else {
                    rt
                };
                if !t.is_integer() {
                    return Err(no_operator(op, lt, rt));
                }
                let r2 = if matches!(op, "<<" | ">>") { Type::INT4 } else { t };
                (t, r2, t)
            }
            "~~" | "!~~" | "~~*" | "!~~*" | "~" | "!~" | "~*" | "!~*" | "^@" => {
                (Type::TEXT, Type::TEXT, Type::BOOL)
            }
            "->" | "->>" => {
                let container = if lt.base == Base::Json { Type::JSON } else { Type::JSONB };
                let key = if rt.is_integer() { Type::INT4 } else { Type::TEXT };
                let ret = if op == "->>" { Type::TEXT } else { container };
                (container, key, ret)
            }
            "#>" | "#>>" => {
                let container = if lt.base == Base::Json { Type::JSON } else { Type::JSONB };
                let ret = if op == "#>>" { Type::TEXT } else { container };
                (container, Type::array_of(Base::Text), ret)
            }
            "@>" | "<@" => {
                if lt.is_range() || rt.is_range() {
                    let range = if lt.is_range() { lt } else { rt };
                    if lt.base == rt.base {
                        (range, range, Type::BOOL)
                    } else {
                        // range @> element / element <@ range: the other
                        // side is a value of the range's own element type.
                        let elem = super::ranges::elem_type(range.base);
                        if lt.is_range() { (lt, elem, Type::BOOL) } else { (elem, rt, Type::BOOL) }
                    }
                } else if lt.array || rt.array {
                    let elem = if lt.array { lt } else { rt };
                    (elem, elem, Type::BOOL)
                } else {
                    (Type::JSONB, Type::JSONB, Type::BOOL)
                }
            }
            "&&" => {
                if lt.is_range() || rt.is_range() {
                    let range = if lt.is_range() { lt } else { rt };
                    (range, range, Type::BOOL)
                } else {
                    let elem = if lt.array { lt } else { rt };
                    (elem, elem, Type::BOOL)
                }
            }
            "?" => (Type::JSONB, Type::TEXT, Type::BOOL),
            "?|" | "?&" => (Type::JSONB, Type::array_of(Base::Text), Type::BOOL),
            "#-" => (Type::JSONB, Type::array_of(Base::Text), Type::JSONB),
            // Either order (`tsvector @@ tsquery` or `tsquery @@ tsvector`);
            // whichever side already resolved to tsquery decides which is
            // which, defaulting to (tsvector, tsquery) when neither has.
            // jsonb @? jsonpath (does it yield anything) / jsonb @@ jsonpath
            // (its single boolean).
            "@?" => (Type::JSONB, Type::TEXT, Type::BOOL),
            "@@" if lt.base == Base::Jsonb => (Type::JSONB, Type::TEXT, Type::BOOL),
            "@@" => {
                if lt.base == Base::Tsquery || rt.base == Base::Tsvector {
                    (Type::TSQUERY, Type::TSVECTOR, Type::BOOL)
                } else {
                    (Type::TSVECTOR, Type::TSQUERY, Type::BOOL)
                }
            }
            _ => return Err(unsupported(&format!("operator {op}"))),
        };
        // jsonb - text / jsonb - int
        let (ltarget, rtarget, ret) = if op == "-" && lt.base == Base::Jsonb {
            (
                Type::JSONB,
                if rt.is_integer() {
                    Type::INT4
                } else if rt.array {
                    Type::array_of(Base::Text)
                } else {
                    Type::TEXT
                },
                Type::JSONB,
            )
        } else {
            (ltarget, rtarget, ret)
        };
        let le = self.coerce(l, ltarget, -1, CastCtx::Implicit, op)?;
        let re = self.coerce(r, rtarget, -1, CastCtx::Implicit, op)?;
        // Constant folding keeps literal arithmetic exact at plan time.
        let e =
            Expr::Call { name: op, args: vec![le, re], ty: ret, arg_tys: vec![ltarget, rtarget] };
        Ok(TE::new(self.fold(e)?, ret).with_typmod(-1))
    }

    /// Evaluates constant expressions at plan time, as Postgres does.
    fn fold(&self, e: Expr) -> PgResult<Expr> {
        let Expr::Call { name, args, ty, arg_tys } = &e else { return Ok(e) };
        if !args.iter().all(|x| matches!(x, Expr::Const(_))) || name.starts_with("random") {
            return Ok(e);
        }
        if matches!(
            *name,
            "now" | "clock_timestamp" | "nextval" | "currval" | "lastval" | "gen_random_uuid"
        ) {
            return Ok(e);
        }
        let vals: Vec<Value> = args
            .iter()
            .map(|x| match x {
                Expr::Const(v) => v.clone(),
                _ => Value::Null,
            })
            .collect();
        let env = self.env();
        let r = match (args.len(), *name) {
            (2, op) if !op.chars().next().is_some_and(|c| c.is_alphabetic()) => {
                if vals.iter().any(Value::is_null) {
                    Ok(Value::Null)
                } else {
                    super::funcs::binop(op, &vals[0], &vals[1], *ty, arg_tys, &env)
                }
            }
            _ => return Ok(e),
        };
        match r {
            Ok(v) => Ok(Expr::Const(v)),
            // Errors surface at run time if the expression is never evaluated.
            Err(_) => Ok(e),
        }
    }

    /// Result and operand types for arithmetic operators.
    fn arith_types(&self, op: &str, lt: Type, rt: Type) -> Option<(Type, Type, Type)> {
        use Base::*;
        let num_rank = |t: Type| match t.base {
            Int2 => Some(1),
            Int4 => Some(2),
            Int8 => Some(3),
            Numeric => Some(4),
            Float4 => Some(5),
            Float8 => Some(6),
            Oid => Some(2),
            _ => None,
        };
        if let (Some(a), Some(b)) = (num_rank(lt), num_rank(rt)) {
            let r = a.max(b);
            let ty = match r {
                1 => Type::INT2,
                2 => Type::INT4,
                3 => Type::INT8,
                4 => Type::NUMERIC,
                5 => Type::FLOAT4,
                _ => Type::FLOAT8,
            };
            // float4 mixed with anything else resolves to float8.
            let ty = if ty.base == Float4 && lt != rt { Type::FLOAT8 } else { ty };
            // ^ is numeric ^ numeric or float8 ^ float8; a float operand
            // wins (numeric casts implicitly to float8, not back).
            if op == "^" {
                let float =
                    matches!(lt.base, Float4 | Float8) || matches!(rt.base, Float4 | Float8);
                return if !float && (lt.base == Numeric || rt.base == Numeric) {
                    Some((Type::NUMERIC, Type::NUMERIC, Type::NUMERIC))
                } else {
                    Some((Type::FLOAT8, Type::FLOAT8, Type::FLOAT8))
                };
            }
            if op == "%" && matches!(ty.base, Float4 | Float8) {
                return None;
            }
            return Some((ty, ty, ty));
        }
        let t = |b: Base| Type::of(b);
        Some(match (op, lt.base, rt.base) {
            ("+", Date, Int2 | Int4) => (t(Date), t(Int4), t(Date)),
            ("+", Int2 | Int4, Date) => (t(Int4), t(Date), t(Date)),
            ("-", Date, Int2 | Int4) => (t(Date), t(Int4), t(Date)),
            ("-", Date, Date) => (t(Date), t(Date), t(Int4)),
            ("+", Date, Interval) | ("-", Date, Interval) => (t(Date), t(Interval), t(Timestamp)),
            ("+", Interval, Date) => (t(Interval), t(Date), t(Timestamp)),
            ("+", Date, Time) => (t(Date), t(Time), t(Timestamp)),
            ("+", Time, Date) => (t(Time), t(Date), t(Timestamp)),
            ("+", Timestamp, Interval) | ("-", Timestamp, Interval) => {
                (t(Timestamp), t(Interval), t(Timestamp))
            }
            ("+", Interval, Timestamp) => (t(Interval), t(Timestamp), t(Timestamp)),
            ("+", Timestamptz, Interval) | ("-", Timestamptz, Interval) => {
                (t(Timestamptz), t(Interval), t(Timestamptz))
            }
            ("+", Interval, Timestamptz) => (t(Interval), t(Timestamptz), t(Timestamptz)),
            ("-", Timestamp, Timestamp) => (t(Timestamp), t(Timestamp), t(Interval)),
            ("-", Timestamptz, Timestamptz) => (t(Timestamptz), t(Timestamptz), t(Interval)),
            ("-", Timestamp, Timestamptz) | ("-", Timestamptz, Timestamp) => {
                (t(Timestamptz), t(Timestamptz), t(Interval))
            }
            ("-", Timestamp, Date) | ("-", Date, Timestamp) => {
                (t(Timestamp), t(Timestamp), t(Interval))
            }
            ("-", Timestamptz, Date) | ("-", Date, Timestamptz) => {
                (t(Timestamptz), t(Timestamptz), t(Interval))
            }
            ("+", Time, Interval) | ("-", Time, Interval) => (t(Time), t(Interval), t(Time)),
            ("+", Interval, Time) => (t(Interval), t(Time), t(Time)),
            ("-", Time, Time) => (t(Time), t(Time), t(Interval)),
            ("+", Interval, Interval) | ("-", Interval, Interval) => {
                (t(Interval), t(Interval), t(Interval))
            }
            ("*", Interval, _) if num_rank(rt).is_some() => (t(Interval), t(Float8), t(Interval)),
            ("*", _, Interval) if num_rank(lt).is_some() => (t(Float8), t(Interval), t(Interval)),
            ("/", Interval, _) if num_rank(rt).is_some() => (t(Interval), t(Float8), t(Interval)),
            ("-", Jsonb, _) => (t(Jsonb), t(Text), t(Jsonb)),
            _ => return None,
        })
    }

    // -----------------------------------------------------------------
    // Functions

    fn bind_function(&mut self, f: &a::Function) -> PgResult<TE> {
        let parts = object_name(&f.name);
        let name = parts.last().cloned().unwrap_or_default();
        if parts.len() > 1
            && !matches!(
                parts[parts.len() - 2].as_str(),
                "pg_catalog" | "public" | "information_schema"
            )
        {
            return Err(PgError::new(
                code::INVALID_SCHEMA_NAME,
                format!("schema \"{}\" does not exist", parts[parts.len() - 2]),
            ));
        }
        // Zero-argument SQL keywords.
        if matches!(f.args, a::FunctionArguments::None) {
            return self.bind_keyword_function(&name);
        }
        if let a::FunctionArguments::Subquery(q) = &f.args {
            // ARRAY(SELECT ...) collects a subquery's rows into an array.
            if name == "array" {
                let (plan, cols) = self.bind_query(q)?;
                let ty = cols.first().map(|c| c.ty).unwrap_or(Type::TEXT).to_array();
                return Ok(TE::new(Expr::Sub { kind: SubKind::Array, query: Box::new(plan) }, ty));
            }
            return Err(unsupported("function with subquery arguments"));
        }
        let a::FunctionArguments::List(list) = &f.args else {
            return Err(unsupported("function with subquery arguments"));
        };
        if let Some(te) = self.bind_sqljson_constructor(&name, list)? {
            return Ok(te);
        }
        let distinct = matches!(list.duplicate_treatment, Some(a::DuplicateTreatment::Distinct));
        let mut star = false;
        let mut args = vec![];
        let mut named: Vec<(String, a::Expr)> = vec![];
        for arg in &list.args {
            match arg {
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(e)) => args.push(e.clone()),
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Wildcard) => star = true,
                a::FunctionArg::Named { name: n, arg: a::FunctionArgExpr::Expr(e), .. } => {
                    named.push((ident(n), e.clone()));
                }
                a::FunctionArg::ExprNamed {
                    name: a::Expr::Identifier(n),
                    arg: a::FunctionArgExpr::Expr(e),
                    ..
                } => {
                    named.push((ident(n), e.clone()));
                }
                _ => return Err(unsupported("function argument")),
            }
        }
        if !named.is_empty() {
            args = place_named_args(&name, args, named)?;
        }
        let mut order_by = vec![];
        let mut sep: Option<a::Expr> = None;
        for c in &list.clauses {
            match c {
                a::FunctionArgumentClause::OrderBy(o) => order_by = o.clone(),
                a::FunctionArgumentClause::Separator(s) => sep = Some(a::Expr::Value(s.clone())),
                a::FunctionArgumentClause::IgnoreOrRespectNulls(_) => {}
                other => return Err(unsupported(&format!("{other}"))),
            }
        }
        if let Some(s) = sep {
            args.push(s);
        }
        // Special forms.
        if name == "row" {
            let mut out = vec![];
            for x in &args {
                out.push(self.bind_expr(x)?.e);
            }
            return Ok(TE::new(Expr::Row(out), Type::RECORD));
        }
        if matches!(name.as_str(), "row_to_json" | "to_json" | "to_jsonb") && args.len() == 1 {
            let te = self.bind_expr(&args[0])?;
            if te.ty.base == Base::Record {
                let names = self.record_field_names(&args[0]);
                let ty = if name == "to_jsonb" { Type::JSONB } else { Type::JSON };
                let name: &'static str =
                    if name == "to_jsonb" { "to_jsonb" } else { "row_to_json" };
                return Ok(TE::new(
                    Expr::Call {
                        name,
                        args: vec![te.e, Expr::Const(names)],
                        ty,
                        arg_tys: vec![Type::RECORD, Type::array_of(Base::Text)],
                    },
                    ty,
                ));
            }
            return self.call(&name, vec![te]);
        }
        match name.as_str() {
            "coalesce" | "nullif" | "greatest" | "least" if f.over.is_none() => {
                let mut tes = vec![];
                for x in &args {
                    tes.push(self.bind_expr(x)?);
                }
                if tes.is_empty() {
                    return Err(PgError::new(
                        code::UNDEFINED_FUNCTION,
                        format!("function {name}() does not exist"),
                    ));
                }
                let tys: Vec<Type> = tes.iter().map(|t| t.ty).collect();
                let ty = self.common_type(&tys, &name.to_uppercase(), 0)?;
                let mut out = vec![];
                for te in tes {
                    out.push(self.coerce(te, ty, -1, CastCtx::Implicit, &name)?);
                }
                if out.iter().any(expr_has_srf) {
                    return Err(PgError::new(
                        code::FEATURE_NOT_SUPPORTED,
                        format!(
                            "set-returning functions are not allowed in {}",
                            name.to_uppercase()
                        ),
                    ));
                }
                let e = match name.as_str() {
                    "coalesce" => Expr::Coalesce(out),
                    "nullif" => {
                        if out.len() != 2 {
                            return Err(PgError::new(
                                code::UNDEFINED_FUNCTION,
                                "function nullif needs 2 arguments",
                            ));
                        }
                        let mut it = out.into_iter();
                        Expr::NullIf(Box::new(it.next().unwrap()), Box::new(it.next().unwrap()))
                    }
                    "greatest" => Expr::Greatest(out, false),
                    _ => Expr::Greatest(out, true),
                };
                return Ok(TE::new(e, ty));
            }
            _ => {}
        }
        let kind = sigs::kind_of(&name);
        // Not a builtin: a user function?
        if kind.is_none()
            && f.over.is_none()
            && let Some(uf) = self.user_function(&name, args.len())
        {
            if uf.returns_set {
                return Err(PgError::new(
                    code::FEATURE_NOT_SUPPORTED,
                    format!("set-returning function {name}() is only supported in FROM"),
                ));
            }
            let mut out = vec![];
            for (i, e) in args.iter().enumerate() {
                let te = self.bind_expr(e)?;
                out.push(self.coerce(te, uf.arg_types[i], -1, CastCtx::Implicit, &name)?);
            }
            return Ok(TE::new(Expr::UserFunc { oid: uf.oid, args: out }, uf.ret));
        }
        let is_agg = kind == Some(Kind::Agg) || (star && name == "count");
        let is_win = kind == Some(Kind::Window);
        if (is_agg || is_win) && f.over.is_none() {
            if is_win {
                return Err(PgError::new(
                    code::WINDOWING_ERROR,
                    format!("window function {name} requires an OVER clause"),
                ));
            }
            if !f.within_group.is_empty() {
                return self.bind_ordered_set(&name, &args, &f.within_group, &f.filter);
            }
            return self.bind_aggregate(&name, &args, star, distinct, &f.filter, &order_by);
        }
        if let Some(over) = &f.over {
            return self
                .bind_window(&name, &args, star, over, &f.filter, is_agg, distinct, &order_by);
        }
        if !order_by.is_empty() {
            return Err(unsupported("ORDER BY in a non-aggregate function call"));
        }
        let mut tes = vec![];
        for x in &args {
            tes.push(self.bind_expr(x)?);
        }
        self.call(&name, tes)
    }

    /// Field names of a record expression: a table's columns, else f1, f2...
    fn record_field_names(&self, e: &a::Expr) -> Value {
        let names: Vec<String> = match e {
            a::Expr::Identifier(id) => {
                let rel = ident(id);
                let cols: Vec<String> = self
                    .scopes
                    .last()
                    .map(|s| {
                        s.cols
                            .iter()
                            .filter(|c| c.rel.as_deref() == Some(rel.as_str()) && !c.hidden)
                            .map(|c| c.name.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                cols
            }
            a::Expr::Tuple(items) => (1..=items.len()).map(|i| format!("f{i}")).collect(),
            a::Expr::Function(f) => {
                let n = match &f.args {
                    a::FunctionArguments::List(l) => l.args.len(),
                    _ => 0,
                };
                (1..=n).map(|i| format!("f{i}")).collect()
            }
            _ => vec![],
        };
        Value::Array(Box::new(super::types::Array::new(
            names.into_iter().map(Value::Text).collect(),
        )))
    }

    fn bind_keyword_function(&mut self, name: &str) -> PgResult<TE> {
        let konst = |v: Value, t: Type| Ok(TE::new(Expr::Const(v), t));
        match name {
            "current_date" => Ok(TE::new(
                Expr::Call { name: "current_date", args: vec![], ty: Type::DATE, arg_tys: vec![] },
                Type::DATE,
            )),
            "current_timestamp" | "now" | "transaction_timestamp" => Ok(TE::new(
                Expr::Call { name: "now", args: vec![], ty: Type::TIMESTAMPTZ, arg_tys: vec![] },
                Type::TIMESTAMPTZ,
            )),
            "localtimestamp" => Ok(TE::new(
                Expr::Call {
                    name: "localtimestamp",
                    args: vec![],
                    ty: Type::TIMESTAMP,
                    arg_tys: vec![],
                },
                Type::TIMESTAMP,
            )),
            "current_time" => Ok(TE::new(
                Expr::Call {
                    name: "current_time",
                    args: vec![],
                    ty: Type::of(Base::Timetz),
                    arg_tys: vec![],
                },
                Type::of(Base::Timetz),
            )),
            "localtime" => Ok(TE::new(
                Expr::Call {
                    name: "localtime",
                    args: vec![],
                    ty: Type::of(Base::Time),
                    arg_tys: vec![],
                },
                Type::of(Base::Time),
            )),
            "session_user" => konst(Value::text(self.sess.user.clone()), Type::NAME),
            "current_user" | "user" | "current_role" => {
                konst(Value::text(self.sess.role.clone()), Type::NAME)
            }
            "current_catalog" | "current_database" => {
                konst(Value::text(self.sess.database.clone()), Type::NAME)
            }
            "current_schema" => Ok(TE::new(
                Expr::Call {
                    name: "current_schema",
                    args: vec![],
                    ty: Type::NAME,
                    arg_tys: vec![],
                },
                Type::NAME,
            )),
            other => self.call(other, vec![]),
        }
    }

    /// Binds a call to a built-in function with already-bound arguments.
    /// Postgres 16's SQL/JSON constructors: `JSON_OBJECT(k VALUE v | k : v,
    /// ... [NULL | ABSENT ON NULL] [RETURNING t])` (NULL ON NULL by default)
    /// and `JSON_ARRAY(v, ... [NULL | ABSENT ON NULL] [RETURNING t])` (ABSENT
    /// ON NULL by default); `None` for the older `json_object(text[])`.
    fn bind_sqljson_constructor(
        &mut self,
        name: &str,
        list: &a::FunctionArgumentList,
    ) -> PgResult<Option<TE>> {
        if !matches!(name, "json_object" | "json_array") {
            return Ok(None);
        }
        let named = list.args.iter().any(|x| {
            matches!(
                x,
                a::FunctionArg::ExprNamed {
                    operator: a::FunctionArgOperator::Value | a::FunctionArgOperator::Colon,
                    ..
                }
            )
        });
        let clauses = list.clauses.iter().any(|c| {
            matches!(
                c,
                a::FunctionArgumentClause::JsonNullClause(_)
                    | a::FunctionArgumentClause::JsonReturningClause(_)
            )
        });
        if name == "json_object" && !named && !clauses && !list.args.is_empty() {
            return Ok(None);
        }
        let mut absent = name == "json_array";
        let mut returning: Option<a::DataType> = None;
        for c in &list.clauses {
            match c {
                a::FunctionArgumentClause::JsonNullClause(n) => {
                    absent = matches!(n, a::JsonNullClause::AbsentOnNull);
                }
                a::FunctionArgumentClause::JsonReturningClause(r) => {
                    returning = Some(r.data_type.clone())
                }
                other => return Err(unsupported(&format!("{other}"))),
            }
        }
        let mut tes = vec![];
        for x in &list.args {
            match x {
                a::FunctionArg::ExprNamed { name: k, arg: a::FunctionArgExpr::Expr(v), .. }
                    if name == "json_object" =>
                {
                    let k = self.bind_expr(k)?;
                    let k = self.coerce(k, Type::TEXT, -1, CastCtx::Assignment, "JSON_OBJECT")?;
                    tes.push(TE::new(k, Type::TEXT));
                    tes.push(self.bind_expr(v)?);
                }
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(v)) if name == "json_array" => {
                    tes.push(self.bind_expr(v)?);
                }
                _ => return Err(unsupported("JSON_OBJECT/JSON_ARRAY argument")),
            }
        }
        let jsonb = match &returning {
            Some(t) => {
                let (ty, _) = self.data_type(t)?;
                ty.base == Base::Jsonb
            }
            None => false,
        };
        let f = match (name, jsonb, absent) {
            ("json_object", false, false) => "json_build_object",
            ("json_object", true, false) => "jsonb_build_object",
            ("json_object", false, true) => "json_build_object_absent",
            ("json_object", true, true) => "jsonb_build_object_absent",
            (_, false, false) => "json_build_array",
            (_, true, false) => "jsonb_build_array",
            (_, false, true) => "json_build_array_absent",
            (_, true, true) => "jsonb_build_array_absent",
        };
        let te = self.call(f, tes)?;
        Ok(Some(match returning {
            Some(t) => {
                let (ty, typmod) = self.data_type(&t)?;
                if matches!(ty.base, Base::Json | Base::Jsonb) {
                    te
                } else {
                    TE::new(self.coerce(te, ty, typmod, CastCtx::Explicit, "RETURNING")?, ty)
                }
            }
            None => te,
        }))
    }

    fn call(&mut self, name: &str, args: Vec<TE>) -> PgResult<TE> {
        let arg_tys: Vec<Type> = args.iter().map(|t| t.ty).collect();
        let r = sigs::resolve(name, &arg_tys)?;
        if r.sig.kind == Kind::Srf {
            // Set-returning function in the select list.
        }
        let mut out = vec![];
        for (te, target) in args.into_iter().zip(r.arg_tys.iter()) {
            out.push(self.coerce(te, *target, -1, CastCtx::Implicit, name)?);
        }
        let e = Expr::Call { name: r.sig.name, args: out, ty: r.ret, arg_tys: r.arg_tys.clone() };
        let mut te = TE::new(self.fold_call(e)?, r.ret);
        if !r.sig.cols.is_empty() {
            let elem = r.arg_tys.first().map(|t| t.elem()).unwrap_or(Type::TEXT);
            te.rec = Some(
                r.sig
                    .cols
                    .iter()
                    .map(|(n, t)| {
                        (n.to_string(), if t.base == Base::AnyElement { elem } else { *t })
                    })
                    .collect(),
            );
        }
        Ok(te)
    }

    fn fold_call(&self, e: Expr) -> PgResult<Expr> {
        let Expr::Call { name, args, ty, arg_tys } = &e else { return Ok(e) };
        let volatile = NOT_IMMUTABLE.contains(name);
        if volatile || !args.iter().all(|x| matches!(x, Expr::Const(_))) {
            return Ok(e);
        }
        let vals: Vec<Value> = args
            .iter()
            .map(|x| match x {
                Expr::Const(v) => v.clone(),
                _ => Value::Null,
            })
            .collect();
        // A strict function of a NULL is NULL (as the executor does).
        let strict = super::sigs::all_sigs()
            .iter()
            .find(|s| s.name == *name)
            .map(|s| s.strict)
            .unwrap_or(true);
        if strict && vals.iter().any(Value::is_null) && !matches!(*name, "subscript" | "slice") {
            return Ok(Expr::Const(Value::Null));
        }
        let env = self.env();
        match super::funcs::call(name, &vals, arg_tys, *ty, &env) {
            Ok(Some(v)) => Ok(Expr::Const(v)),
            _ => Ok(e),
        }
    }

    fn bind_aggregate(
        &mut self,
        name: &str,
        args: &[a::Expr],
        star: bool,
        distinct: bool,
        filter: &Option<Box<a::Expr>>,
        order_by: &[a::OrderByExpr],
    ) -> PgResult<TE> {
        if self.frames.is_empty() {
            return Err(PgError::new(
                code::GROUPING_ERROR,
                "aggregate functions are not allowed here",
            ));
        }
        if let Some(what) = self.frames.last().unwrap().forbid {
            return Err(PgError::new(
                code::GROUPING_ERROR,
                format!("aggregate functions are not allowed in {what}"),
            ));
        }
        // Aggregate arguments live in the input scope and may not nest aggregates.
        self.frames.push(AggFrame { forbid: Some("an aggregate argument"), ..Default::default() });
        let bound: PgResult<Vec<TE>> = args.iter().map(|x| self.bind_expr(x)).collect();
        let filter_te = filter.as_ref().map(|f| self.bind_expr(f));
        let order_te: PgResult<Vec<(TE, bool, bool)>> = order_by
            .iter()
            .map(|o| {
                let te = self.bind_expr(&o.expr)?;
                let desc = o.options.sort == Some(a::OrderBySort::Desc);
                Ok((te, desc, o.options.nulls_first.unwrap_or(desc)))
            })
            .collect();
        self.frames.pop();
        let bound = bound?;
        let order_te = order_te?;
        let arg_tys: Vec<Type> = bound.iter().map(|t| t.ty).collect();
        let r = if star && name == "count" {
            sigs::resolve("count", &[])?
        } else {
            sigs::resolve(name, &arg_tys)?
        };
        if r.sig.kind != Kind::Agg {
            return Err(PgError::new(
                code::WRONG_OBJECT_TYPE,
                format!("{name} is not an aggregate function"),
            ));
        }
        let mut out = vec![];
        for (te, target) in bound.into_iter().zip(r.arg_tys.iter()) {
            out.push(self.coerce(te, *target, -1, CastCtx::Implicit, name)?);
        }
        let filter_e = match filter_te {
            Some(t) => Some(self.bool_expr(t?, "FILTER")?),
            None => None,
        };
        let order = order_te.into_iter().map(|(te, d, n)| (te.e, d, n)).collect();
        // min/max over an enum: compare (sort position, label) records,
        // then take the label back out.
        let enum_minmax = matches!(r.sig.name, "min" | "max")
            && r.arg_tys.first().is_some_and(|t| matches!(t.base, Base::Enum(_)) && !t.array);
        let (out, arg_tys) = if enum_minmax {
            let t = r.arg_tys[0];
            let arg = out.into_iter().next().unwrap();
            let Base::Enum(oid) = t.base else { unreachable!() };
            // Strict: a NULL label stays NULL, so the aggregate skips it.
            let key = Expr::Call {
                name: "__enum_key",
                args: vec![Expr::Const(Value::Int(oid as i64)), arg],
                ty: Type::RECORD,
                arg_tys: vec![Type::INT4, t],
            };
            (vec![key], vec![Type::RECORD])
        } else {
            (out, r.arg_tys.clone())
        };
        let call = AggCall {
            name: r.sig.name,
            args: out,
            arg_tys,
            ty: r.ret,
            distinct,
            filter: filter_e,
            order,
            star,
            direct: None,
        };
        let frame = self.frames.last_mut().unwrap();
        let idx = match frame.aggs.iter().position(|x| *x == call) {
            Some(i) => i,
            None => {
                frame.aggs.push(call);
                frame.aggs.len() - 1
            }
        };
        if enum_minmax {
            return Ok(TE::new(
                Expr::Call {
                    name: "record_field",
                    args: vec![Expr::AggRef(idx), Expr::Const(Value::Int(1))],
                    ty: r.ret,
                    arg_tys: vec![Type::RECORD, Type::INT4],
                },
                r.ret,
            ));
        }
        Ok(TE::new(Expr::AggRef(idx), r.ret))
    }

    /// `mode() / percentile_cont(f) / percentile_disc(f) WITHIN GROUP
    /// (ORDER BY x)`: aggregated over x in that order, with `f` (a fraction
    /// or an array of them) as the direct argument.
    fn bind_ordered_set(
        &mut self,
        name: &str,
        args: &[a::Expr],
        within: &[a::OrderByExpr],
        filter: &Option<Box<a::Expr>>,
    ) -> PgResult<TE> {
        let fname: &'static str = match name {
            "mode" => "mode",
            "percentile_cont" => "percentile_cont",
            "percentile_disc" => "percentile_disc",
            other => {
                return Err(PgError::new(
                    code::WRONG_OBJECT_TYPE,
                    format!(
                        "{other} is not an ordered-set aggregate, so it cannot have WITHIN GROUP"
                    ),
                ));
            }
        };
        if self.frames.is_empty() || self.frames.last().unwrap().forbid.is_some() {
            return Err(PgError::new(
                code::GROUPING_ERROR,
                "aggregate functions are not allowed here",
            ));
        }
        if within.len() != 1 {
            return Err(PgError::new(
                code::UNDEFINED_FUNCTION,
                format!("function {name} must have exactly one ORDER BY column"),
            ));
        }
        self.frames.push(AggFrame { forbid: Some("an aggregate argument"), ..Default::default() });
        let data = self.bind_expr(&within[0].expr);
        let direct = args.first().map(|x| self.bind_expr(x));
        let filter_te = filter.as_ref().map(|f| self.bind_expr(f));
        self.frames.pop();
        let mut data = data?;
        let desc = within[0].options.sort == Some(a::OrderBySort::Desc);
        let nulls_first = within[0].options.nulls_first.unwrap_or(desc);
        let (direct, ret) = match fname {
            "mode" => {
                if direct.is_some() {
                    return Err(PgError::new(
                        code::UNDEFINED_FUNCTION,
                        "function mode takes no direct arguments",
                    ));
                }
                (None, data.ty)
            }
            _ => {
                let Some(d) = direct else {
                    return Err(PgError::new(
                        code::UNDEFINED_FUNCTION,
                        format!("function {name} needs a fraction"),
                    ));
                };
                let d = d?;
                let is_array = d.ty.array;
                let target = if is_array { Type::array_of(Base::Float8) } else { Type::FLOAT8 };
                let d = self.coerce(d, target, -1, CastCtx::Implicit, name)?;
                let elem = if fname == "percentile_cont" {
                    data = TE::new(
                        self.coerce(data, Type::FLOAT8, -1, CastCtx::Implicit, name)?,
                        Type::FLOAT8,
                    );
                    Type::FLOAT8
                } else {
                    data.ty
                };
                (Some(d), Type { base: elem.base, array: is_array })
            }
        };
        let filter = match filter_te {
            Some(t) => Some(self.bool_expr(t?, "FILTER")?),
            None => None,
        };
        let call = AggCall {
            name: fname,
            args: vec![data.e.clone()],
            arg_tys: vec![data.ty],
            ty: ret,
            distinct: false,
            filter,
            order: vec![(data.e, desc, nulls_first)],
            star: false,
            direct,
        };
        let frame = self.frames.last_mut().unwrap();
        let idx = match frame.aggs.iter().position(|x| *x == call) {
            Some(i) => i,
            None => {
                frame.aggs.push(call);
                frame.aggs.len() - 1
            }
        };
        Ok(TE::new(Expr::AggRef(idx), ret))
    }

    #[allow(clippy::too_many_arguments)]
    fn bind_window(
        &mut self,
        name: &str,
        args: &[a::Expr],
        star: bool,
        over: &a::WindowType,
        filter: &Option<Box<a::Expr>>,
        is_agg: bool,
        distinct: bool,
        order_by: &[a::OrderByExpr],
    ) -> PgResult<TE> {
        let resolved = self.resolve_window(over)?;
        let spec = &resolved;
        if self.window_depth > 0 {
            return Err(PgError::new(
                code::WINDOWING_ERROR,
                "window function calls cannot be nested",
            ));
        }
        if self.frames.is_empty() || self.frames.last().unwrap().forbid.is_some() {
            return Err(PgError::new(
                code::WINDOWING_ERROR,
                format!(
                    "window functions are not allowed in {}",
                    self.frames.last().and_then(|f| f.forbid).unwrap_or("this context")
                ),
            ));
        }
        // Windows run after grouping: an aggregate in the arguments, PARTITION
        // BY or ORDER BY is the query's own (`sum(sum(x)) OVER ()`).
        self.window_depth += 1;
        let bound: PgResult<Vec<TE>> = args.iter().map(|x| self.bind_expr(x)).collect();
        let partition: PgResult<Vec<TE>> =
            spec.partition_by.iter().map(|x| self.bind_expr(x)).collect();
        let order: PgResult<Vec<(TE, bool, bool)>> = spec
            .order_by
            .iter()
            .chain(order_by)
            .map(|o| {
                let te = self.bind_expr(&o.expr)?;
                let desc = o.options.sort == Some(a::OrderBySort::Desc);
                Ok((te, desc, o.options.nulls_first.unwrap_or(desc)))
            })
            .collect();
        let filter_te = filter.as_ref().map(|f| self.bind_expr(f));
        self.window_depth -= 1;
        let bound = bound?;
        let arg_tys: Vec<Type> = bound.iter().map(|t| t.ty).collect();
        let r = if star && name == "count" {
            sigs::resolve("count", &[])?
        } else {
            sigs::resolve(name, &arg_tys)?
        };
        let mut out = vec![];
        for (te, target) in bound.into_iter().zip(r.arg_tys.iter()) {
            out.push(self.coerce(te, *target, -1, CastCtx::Implicit, name)?);
        }
        let frame = match &spec.window_frame {
            Some(f) => Some(self.bind_frame(f)?),
            None => None,
        };
        let agg = if is_agg {
            Some(AggCall {
                name: r.sig.name,
                args: out.clone(),
                arg_tys: r.arg_tys.clone(),
                ty: r.ret,
                distinct,
                filter: match filter_te {
                    Some(t) => Some(self.bool_expr(t?, "FILTER")?),
                    None => None,
                },
                order: vec![],
                star,
                direct: None,
            })
        } else {
            None
        };
        let call = WinCall {
            name: r.sig.name,
            args: out,
            arg_tys: r.arg_tys.clone(),
            ty: r.ret,
            partition: partition?.into_iter().map(|t| t.e).collect(),
            order: order?.into_iter().map(|(te, d, n)| (te.e, d, n)).collect(),
            frame,
            agg,
        };
        let f = self.frames.last_mut().unwrap();
        f.wins.push(call);
        Ok(TE::new(Expr::WinRef(f.wins.len() - 1), r.ret))
    }

    fn bind_frame(&mut self, f: &a::WindowFrame) -> PgResult<Frame> {
        // RANGE offsets are values of the ORDER BY key's type (a number, or
        // an interval for a date/timestamp key); ROWS/GROUPS offsets count.
        let range = matches!(f.units, a::WindowFrameUnits::Range);
        let bound = |b: &a::WindowFrameBound, me: &mut Self| -> PgResult<FrameBound> {
            Ok(match b {
                a::WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                a::WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                a::WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                a::WindowFrameBound::Preceding(Some(e)) if range => {
                    FrameBound::Preceding(me.bind_expr(e)?.e)
                }
                a::WindowFrameBound::Following(Some(e)) if range => {
                    FrameBound::Following(me.bind_expr(e)?.e)
                }
                a::WindowFrameBound::Preceding(Some(e)) => {
                    let te = me.bind_expr(e)?;
                    FrameBound::Preceding(me.coerce(
                        te,
                        Type::INT8,
                        -1,
                        CastCtx::Assignment,
                        "frame",
                    )?)
                }
                a::WindowFrameBound::Following(Some(e)) => {
                    let te = me.bind_expr(e)?;
                    FrameBound::Following(me.coerce(
                        te,
                        Type::INT8,
                        -1,
                        CastCtx::Assignment,
                        "frame",
                    )?)
                }
            })
        };
        let rows = matches!(f.units, a::WindowFrameUnits::Rows);
        let groups = matches!(f.units, a::WindowFrameUnits::Groups);
        let start = bound(&f.start_bound, self)?;
        let end = match &f.end_bound {
            Some(b) => bound(b, self)?,
            None => FrameBound::CurrentRow,
        };
        if matches!(start, FrameBound::UnboundedFollowing) {
            return Err(PgError::new(
                code::WINDOWING_ERROR,
                "frame start cannot be UNBOUNDED FOLLOWING",
            ));
        }
        if matches!(end, FrameBound::UnboundedPreceding) {
            return Err(PgError::new(
                code::WINDOWING_ERROR,
                "frame end cannot be UNBOUNDED PRECEDING",
            ));
        }
        Ok(Frame { rows, groups, start, end })
    }

    // -----------------------------------------------------------------
    // Types and coercion

    pub fn data_type(&self, dt: &a::DataType) -> PgResult<(Type, i32)> {
        use a::DataType as D;
        let char_len = |c: &Option<a::CharacterLength>| -> Option<i64> {
            match c {
                Some(a::CharacterLength::IntegerLength { length, .. }) => Some(*length as i64),
                _ => None,
            }
        };
        let num_mod = |e: &a::ExactNumberInfo| -> PgResult<i32> {
            match e {
                a::ExactNumberInfo::None => Ok(-1),
                a::ExactNumberInfo::Precision(p) => {
                    types::encode_typmod(Base::Numeric, &[*p as i64])
                }
                a::ExactNumberInfo::PrecisionAndScale(p, s) => {
                    types::encode_typmod(Base::Numeric, &[*p as i64, *s])
                }
            }
        };
        Ok(match dt {
            D::Boolean | D::Bool => (Type::BOOL, -1),
            D::SmallInt(_) | D::Int2(_) => (Type::INT2, -1),
            D::Int(_) | D::Integer(_) | D::Int4(_) => (Type::INT4, -1),
            D::BigInt(_) | D::Int8(_) => (Type::INT8, -1),
            D::Real | D::Float4 => (Type::FLOAT4, -1),
            D::DoublePrecision | D::Float8 | D::Float64 => (Type::FLOAT8, -1),
            D::Float(p) => match p {
                a::ExactNumberInfo::Precision(n) if *n <= 24 => (Type::FLOAT4, -1),
                _ => (Type::FLOAT8, -1),
            },
            D::Numeric(e) | D::Decimal(e) | D::Dec(e) => (Type::NUMERIC, num_mod(e)?),
            D::Text => (Type::TEXT, -1),
            D::Varchar(l) | D::CharacterVarying(l) | D::CharVarying(l) | D::Nvarchar(l) => (
                Type::VARCHAR,
                match char_len(l) {
                    Some(n) => types::encode_typmod(Base::Varchar, &[n])?,
                    None => -1,
                },
            ),
            D::Char(l) | D::Character(l) => (
                Type::of(Base::Bpchar),
                types::encode_typmod(Base::Bpchar, &[char_len(l).unwrap_or(1)])?,
            ),
            D::Bytea => (Type::BYTEA, -1),
            D::Date => (Type::DATE, -1),
            D::Time(p, tz) => {
                let t = if matches!(tz, a::TimezoneInfo::WithTimeZone | a::TimezoneInfo::Tz) {
                    Type::of(Base::Timetz)
                } else {
                    Type::of(Base::Time)
                };
                (t, p.map(|p| p as i32).unwrap_or(-1))
            }
            D::Timestamp(p, tz) => {
                let t = if matches!(tz, a::TimezoneInfo::WithTimeZone | a::TimezoneInfo::Tz) {
                    Type::TIMESTAMPTZ
                } else {
                    Type::TIMESTAMP
                };
                (t, p.map(|p| p as i32).unwrap_or(-1))
            }
            D::Datetime(p) => (Type::TIMESTAMP, p.map(|p| p as i32).unwrap_or(-1)),
            D::Interval { .. } => (Type::INTERVAL, -1),
            D::JSON => (Type::JSON, -1),
            D::JSONB => (Type::JSONB, -1),
            D::Uuid => (Type::UUID, -1),
            D::TsVector => (Type::TSVECTOR, -1),
            D::TsQuery => (Type::TSQUERY, -1),
            D::Regclass => (Type::of(Base::Regclass), -1),
            D::Array(def) => {
                let inner = match def {
                    a::ArrayElemTypeDef::SquareBracket(t, _)
                    | a::ArrayElemTypeDef::AngleBracket(t)
                    | a::ArrayElemTypeDef::Parenthesis(t)
                    | a::ArrayElemTypeDef::Qualified(t, _) => self.data_type(t)?,
                    a::ArrayElemTypeDef::None => (Type::TEXT, -1),
                };
                (inner.0.to_array(), inner.1)
            }
            D::Custom(name, mods) => {
                let parts = object_name(name);
                let last = parts.last().cloned().unwrap_or_default();
                let args: Vec<i64> = mods.iter().filter_map(|m| m.trim().parse().ok()).collect();
                self.named_type(&parts, &last, &args)?
            }
            other => return Err(unsupported(&format!("type {other}"))),
        })
    }

    fn named_type(&self, parts: &[String], name: &str, args: &[i64]) -> PgResult<(Type, i32)> {
        // Built-in type names sqlparser hands through as Custom.
        let base = match name {
            "text" => Some(Base::Text),
            "numeric" | "decimal" => Some(Base::Numeric),
            "date" => Some(Base::Date),
            "time" => Some(Base::Time),
            "timestamp" => Some(Base::Timestamp),
            "boolean" => Some(Base::Bool),
            "real" => Some(Base::Float4),
            "double precision" => Some(Base::Float8),
            "character" => Some(Base::Bpchar),
            "character varying" => Some(Base::Varchar),
            "bytea" if false => None,
            "cstring" => Some(Base::Cstring),
            "unknown" => Some(Base::Unknown),
            "refcursor" => Some(Base::Refcursor),
            "tid" => Some(Base::Tid),
            "cid" => Some(Base::Cid),
            "regprocedure" => Some(Base::Regprocedure),
            "regoper" => Some(Base::Regoper),
            "regoperator" => Some(Base::Regoperator),
            "regconfig" => Some(Base::Regconfig),
            "trigger" => Some(Base::Trigger),
            "internal" => Some(Base::Internal),
            "anynonarray" => Some(Base::AnyNonArray),
            "anyenum" => Some(Base::AnyEnum),
            "any" => Some(Base::Any),
            "int2" | "smallint" => Some(Base::Int2),
            "int4" | "int" | "integer" => Some(Base::Int4),
            "int8" | "bigint" => Some(Base::Int8),
            "float4" => Some(Base::Float4),
            "float8" => Some(Base::Float8),
            "bool" => Some(Base::Bool),
            "name" => Some(Base::Name),
            "oid" => Some(Base::Oid),
            "bpchar" => Some(Base::Bpchar),
            "varchar" => Some(Base::Varchar),
            "timestamptz" => Some(Base::Timestamptz),
            "timetz" => Some(Base::Timetz),
            "serial" | "serial4" => Some(Base::Int4),
            "bigserial" | "serial8" => Some(Base::Int8),
            "smallserial" | "serial2" => Some(Base::Int2),
            "money" => Some(Base::Money),
            "inet" => Some(Base::Inet),
            "cidr" => Some(Base::Cidr),
            "macaddr" => Some(Base::Macaddr),
            "xml" => Some(Base::Xml),
            "citext" => Some(Base::Text),
            "regclass" => Some(Base::Regclass),
            "regtype" => Some(Base::Regtype),
            "regproc" => Some(Base::Regproc),
            "regnamespace" => Some(Base::Regnamespace),
            "regrole" => Some(Base::Regrole),
            "tsvector" => Some(Base::Tsvector),
            "tsquery" => Some(Base::Tsquery),
            "int4range" => Some(Base::Int4Range),
            "int8range" => Some(Base::Int8Range),
            "numrange" => Some(Base::NumRange),
            "daterange" => Some(Base::DateRange),
            "tsrange" => Some(Base::TsRange),
            "tstzrange" => Some(Base::TstzRange),
            "pg_lsn" => Some(Base::PgLsn),
            "jsonb" => Some(Base::Jsonb),
            "json" => Some(Base::Json),
            "uuid" => Some(Base::Uuid),
            "bytea" => Some(Base::Bytea),
            "interval" => Some(Base::Interval),
            "char" => Some(Base::Char),
            "anyelement" => Some(Base::AnyElement),
            "anyarray" => Some(Base::AnyArray),
            "void" => Some(Base::Void),
            "record" => Some(Base::Record),
            "int2vector" => Some(Base::Int2Vector),
            "oidvector" => Some(Base::OidVector),
            "pg_node_tree" => Some(Base::PgNodeTree),
            "aclitem" => Some(Base::Aclitem),
            "xid" => Some(Base::Xid),
            _ => None,
        };
        if let Some(b) = base {
            let typmod = if args.is_empty() { -1 } else { types::encode_typmod(b, args)? };
            return Ok((Type { base: b, array: false }, typmod));
        }
        // User-defined enum types.
        let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        let schemas: Vec<String> = match schema {
            Some(s) => vec![s],
            None => self.sess.search_path.clone(),
        };
        for s in schemas {
            if let Some(sid) = self.db.schema_by_name(&s)
                && let Some(e) = self.db.find_enum(sid, name)
            {
                return Ok((Type { base: Base::Enum(e.oid), array: false }, -1));
            }
        }
        Err(PgError::new(code::UNDEFINED_OBJECT, format!("type \"{name}\" does not exist")))
    }

    /// The common type of a list, as UNION/CASE/ARRAY resolve it.
    pub fn common_type(&self, tys: &[Type], context: &str, _col: usize) -> PgResult<Type> {
        let known: Vec<Type> = tys.iter().copied().filter(|t| !t.is_unknown()).collect();
        if known.is_empty() {
            return Ok(Type::TEXT);
        }
        let mut best = known[0];
        for &t in &known[1..] {
            if t == best {
                continue;
            }
            best = match promote(best, t) {
                Some(p) => p,
                None => {
                    return Err(PgError::new(
                        code::DATATYPE_MISMATCH,
                        format!(
                            "{context} types {} and {} cannot be matched",
                            best.display(-1),
                            t.display(-1)
                        ),
                    ));
                }
            };
        }
        Ok(best)
    }

    fn cast_to(&self, e: Expr, from: Type, to: Type) -> PgResult<Expr> {
        if from == to {
            return Ok(e);
        }
        Ok(Expr::Cast { expr: Box::new(e), from, to, typmod: -1, explicit: false })
    }

    /// Coerces a bound expression to `target`, as Postgres would in `ctx`.
    pub fn coerce(
        &mut self,
        te: TE,
        target: Type,
        typmod: i32,
        ctx: CastCtx,
        what: &str,
    ) -> PgResult<Expr> {
        if target.base == Base::Any {
            return Ok(te.e);
        }
        if target.base == Base::AnyElement && !target.array {
            if te.ty.is_unknown() {
                return self.coerce(te, Type::TEXT, typmod, ctx, what);
            }
            return Ok(te.e);
        }
        if te.ty == target {
            if typmod >= 0 {
                if let Expr::Const(v) = &te.e {
                    return Ok(Expr::Const(types::apply_typmod(
                        v.clone(),
                        target,
                        typmod,
                        ctx == CastCtx::Explicit,
                    )?));
                }
                return Ok(Expr::Cast {
                    expr: Box::new(te.e),
                    from: target,
                    to: target,
                    typmod,
                    explicit: ctx == CastCtx::Explicit,
                });
            }
            return Ok(te.e);
        }
        if target.is_reg()
            && let Expr::Const(Value::Text(s)) = &te.e
        {
            let oid = pgcatalog::resolve_reg(
                self.db,
                &self.sess.search_path,
                &self.sess.user,
                target.base,
                s,
            )
            .ok_or_else(|| pgcatalog::undefined_reg(target.base, s))?;
            return Ok(Expr::Const(Value::Int(oid)));
        }
        if te.ty.is_unknown() {
            match &te.e {
                Expr::Const(Value::Null) => return Ok(Expr::Const(Value::Null)),
                Expr::Const(Value::Text(s)) => {
                    if let (Base::Enum(oid), false) = (target.base, target.array)
                        && let Some(e) = self.db.enums.get(&oid)
                        && !e.labels.iter().any(|(_, l, _)| l == s)
                    {
                        return Err(PgError::new(
                            code::INVALID_TEXT_REPRESENTATION,
                            format!(
                                "invalid input value for enum {}: \"{s}\"",
                                super::funcs::quote_ident(&e.name)
                            ),
                        ));
                    }
                    let v = types::from_text(s, target, &self.dctx())?;
                    let v = types::apply_typmod(v, target, typmod, ctx == CastCtx::Explicit)?;
                    return Ok(Expr::Const(v));
                }
                Expr::Param(i) => {
                    let i = *i;
                    if self.params[i].is_unknown() {
                        self.params[i] = target;
                    }
                    // Re-run through the ordinary (now not-unknown) path
                    // below rather than returning bare `Expr::Param(i)`:
                    // even when the locked type already equals `target`,
                    // this still needs to apply `typmod` (e.g. a
                    // `numeric(8,2)` column's scale), which only the
                    // `Expr::Cast` wrapping below does.
                    let from = self.params[i];
                    return self.coerce(TE::new(Expr::Param(i), from), target, typmod, ctx, what);
                }
                _ => {
                    return Ok(Expr::Cast {
                        expr: Box::new(te.e),
                        from: Type::UNKNOWN,
                        to: target,
                        typmod,
                        explicit: ctx == CastCtx::Explicit,
                    });
                }
            }
        }
        match casts::cast_context(te.ty, target) {
            Some(c) if c <= ctx => {}
            _ => {
                return Err(cannot_coerce(te.ty, target, ctx, what));
            }
        }
        // Fold constant casts so errors surface at plan time like Postgres.
        // (A reg* value printed as text needs the catalog at run time.)
        if let Expr::Const(v) = &te.e
            && !(te.ty.is_reg() && (target.is_string() || target.base == Base::Text))
        {
            let folded = casts::cast(
                v.clone(),
                te.ty,
                target,
                typmod,
                ctx == CastCtx::Explicit,
                self.fmt(),
                self.sess.now,
            )?;
            return Ok(Expr::Const(folded));
        }
        Ok(Expr::Cast {
            expr: Box::new(te.e),
            from: te.ty,
            to: target,
            typmod,
            explicit: ctx == CastCtx::Explicit,
        })
    }

    /// Postgres's FigureColname.
    fn column_name(&self, e: &a::Expr, te: &TE) -> String {
        let (name, _) = self.colname_strength(e, te);
        name
    }

    /// Postgres's FigureIndexColname: the name an index expression gives a
    /// generated index name (`expr` when it has none).
    pub fn index_column_name(&self, e: &a::Expr) -> String {
        let te = TE::new(Expr::Const(Value::Null), Type::TEXT);
        match self.colname_strength(e, &te) {
            (_, 0) => "expr".into(),
            (n, _) => n,
        }
    }

    /// (name, strength): 2 is a name of its own, 1 a fallback from a cast or
    /// CASE, 0 none at all.
    fn colname_strength(&self, e: &a::Expr, te: &TE) -> (String, u8) {
        let named = |n: &str| (n.to_string(), 2u8);
        match e {
            a::Expr::Identifier(id) => named(&ident(id)),
            a::Expr::CompoundIdentifier(ids) => named(&ids.last().map(ident).unwrap_or_default()),
            a::Expr::Nested(x) => self.colname_strength(x, te),
            a::Expr::Collate { expr, .. } => self.colname_strength(expr, te),
            a::Expr::Cast { expr, data_type, .. } => {
                let (inner, strength) = self.colname_strength(expr, te);
                if strength <= 1 {
                    let n = self
                        .data_type(data_type)
                        .map(|(t, _)| t.elem().name())
                        .unwrap_or_else(|_| "?column?".into());
                    (n, 1)
                } else {
                    (inner, strength)
                }
            }
            a::Expr::Case { else_result, .. } => match else_result {
                Some(x) => {
                    let (inner, strength) = self.colname_strength(x, te);
                    if strength <= 1 { ("case".into(), 1) } else { (inner, strength) }
                }
                None => ("case".into(), 1),
            },
            other => {
                let n = self.column_name_basic(other, te);
                if n == "?column?" { (n, 0) } else { (n, 2) }
            }
        }
    }

    fn column_name_basic(&self, e: &a::Expr, te: &TE) -> String {
        let _ = te;
        use a::Expr as E;
        match e {
            E::Function(f) => object_name(&f.name).pop().unwrap_or_default(),
            E::Exists { .. } => "exists".into(),
            E::Array(_) => "array".into(),
            E::Tuple(_) => "row".into(),
            E::Value(_) => "?column?".into(),
            E::TypedString(ts) => self
                .data_type(&ts.data_type)
                .map(|(t, _)| t.elem().name())
                .unwrap_or_else(|_| "?column?".into()),
            E::Interval(_) => "interval".into(),
            E::Extract { .. } => "extract".into(),
            E::Substring { shorthand, .. } => {
                if *shorthand { "substr" } else { "substring" }.into()
            }
            E::Trim { trim_where, .. } => match trim_where {
                Some(a::TrimWhereField::Leading) => "ltrim".into(),
                Some(a::TrimWhereField::Trailing) => "rtrim".into(),
                _ => "btrim".into(),
            },
            E::Position { .. } => "position".into(),
            E::Overlay { .. } => "overlay".into(),
            E::Ceil { .. } => "ceil".into(),
            E::Floor { .. } => "floor".into(),
            E::IsNull(_) | E::IsNotNull(_) => "?column?".into(),
            E::Subquery(q) => {
                // The subquery's first output column name.
                if let a::SetExpr::Select(s) = q.body.as_ref()
                    && let Some(item) = s.projection.first()
                {
                    return match item {
                        a::SelectItem::ExprWithAlias { alias, .. } => ident(alias),
                        a::SelectItem::UnnamedExpr(e) => self.column_name(e, te),
                        _ => "?column?".into(),
                    };
                }
                "?column?".into()
            }
            E::CompoundFieldAccess { root, access_chain } => {
                if access_chain.iter().all(|c| matches!(c, a::AccessExpr::Subscript(_))) {
                    self.column_name(root, te)
                } else {
                    "?column?".into()
                }
            }
            E::AtTimeZone { .. } => "timezone".into(),
            _ => "?column?".into(),
        }
    }

    // -----------------------------------------------------------------
    // DML

    fn bind_insert(&mut self, ins: &a::Insert) -> PgResult<Planned> {
        let a::TableObject::TableName(name) = &ins.table else {
            return Err(unsupported("INSERT into a function or query"));
        };
        let parts = object_name(name);
        let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        let tname = parts.last().cloned().unwrap_or_default();
        let oid = self.lookup_table_oid(schema.as_deref(), &tname)?;
        let table = self.db.table(oid).unwrap();
        if table.kind != RelKind::Table {
            return Err(PgError::new(
                code::WRONG_OBJECT_TYPE,
                format!("cannot insert into view \"{tname}\""),
            )
            .detail("Views that do not select from a single table or view are not automatically updatable."));
        }
        // Target columns.
        let cols: Vec<usize> = if ins.columns.is_empty() {
            table.live_columns().filter(|(_, c)| c.generated.is_none()).map(|(i, _)| i).collect()
        } else {
            let mut seen = vec![];
            for c in &ins.columns {
                let n = object_name(c).pop().unwrap_or_default();
                let idx = table.col_index(&n).ok_or_else(|| {
                    PgError::new(
                        code::UNDEFINED_COLUMN,
                        format!("column \"{n}\" of relation \"{tname}\" does not exist"),
                    )
                })?;
                if seen.contains(&idx) {
                    return Err(PgError::new(
                        code::DUPLICATE_COLUMN,
                        format!("column \"{n}\" specified more than once"),
                    ));
                }
                seen.push(idx);
            }
            seen
        };
        // Without a column list, the targets are the table's *first* N
        // columns, N being the source's width (`INSERT INTO t VALUES ('a', 1)`
        // into a three-column table defaults the third). Found via testing
        // before a public release: this used to be rejected.
        let mut cols = cols;
        if ins.columns.is_empty()
            && let Some(width) = ins.source.as_deref().and_then(source_width)
            && width < cols.len()
        {
            cols.truncate(width);
        }
        let target_types: Vec<(Type, i32)> =
            cols.iter().map(|&i| (table.columns[i].ty, table.columns[i].typmod)).collect();
        // Source rows.
        let source = match &ins.source {
            Some(q) => {
                let (plan, scols) = self.bind_insert_source(q, &target_types, table, &cols)?;
                if scols.len() != cols.len() {
                    return Err(PgError::new(
                        code::SYNTAX_ERROR,
                        if scols.len() > cols.len() {
                            "INSERT has more expressions than target columns"
                        } else {
                            "INSERT has more target columns than expressions"
                        },
                    ));
                }
                plan
            }
            None => Query::Values { rows: vec![vec![]], order: vec![], limit: None, offset: None },
        };
        let defaults = self.column_defaults(table)?;
        // ON CONFLICT
        let on_conflict = match &ins.on {
            None => None,
            Some(a::OnInsert::OnConflict(oc)) => Some(self.bind_on_conflict(oc, table, oid)?),
            Some(_) => return Err(unsupported("ON DUPLICATE KEY UPDATE")),
        };
        let (returning, rcols) = self.bind_returning(&ins.returning, table, oid)?;
        Ok(Planned {
            query: Query::Dml(Box::new(Dml::Insert {
                table: oid,
                cols,
                source,
                defaults,
                on_conflict,
                returning,
                overriding_system: ins.overwrite,
            })),
            cols: rcols,
            tag: "INSERT",
            cte_slots: self.cte_slots,
            returns_rows: ins.returning.is_some(),
        })
    }

    /// Binds INSERT's source, coercing to the target column types.
    fn bind_insert_source(
        &mut self,
        q: &a::Query,
        targets: &[(Type, i32)],
        table: &Table,
        cols: &[usize],
    ) -> PgResult<(Query, Vec<OutCol>)> {
        if let a::SetExpr::Values(v) = q.body.as_ref()
            && q.with.is_none()
        {
            let mut rows = vec![];
            self.scopes.push(Scope::default());
            self.frames.push(AggFrame { forbid: Some("VALUES"), ..Default::default() });
            let width = v.rows.first().map_or(0, |r| r.content.len());
            let r = (|| -> PgResult<()> {
                for row in &v.rows {
                    if row.content.len() != width {
                        return Err(PgError::new(
                            code::SYNTAX_ERROR,
                            "VALUES lists must all be the same length",
                        ));
                    }
                    let mut out = vec![];
                    for (i, e) in row.content.iter().enumerate() {
                        if matches!(e, a::Expr::Identifier(id) if ident(id) == "default") {
                            out.push(Expr::Default(*cols.get(i).unwrap_or(&0)));
                            continue;
                        }
                        let te = self.bind_expr(e)?;
                        let (ty, typmod) = targets.get(i).copied().unwrap_or((Type::TEXT, -1));
                        let colname =
                            cols.get(i).map(|&c| table.columns[c].name.clone()).unwrap_or_default();
                        out.push(self.coerce_assign(te, ty, typmod, &colname, &table.name)?);
                    }
                    rows.push(out);
                }
                Ok(())
            })();
            self.frames.pop();
            self.scopes.pop();
            r?;
            let ocols = (0..width)
                .map(|i| {
                    OutCol::new(
                        format!("column{}", i + 1),
                        targets.get(i).map_or(Type::TEXT, |t| t.0),
                    )
                })
                .collect();
            return Ok((Query::Values { rows, order: vec![], limit: None, offset: None }, ocols));
        }
        // `INSERT INTO t (...) SELECT $1, $2, ... WHERE ...` (as opposed to
        // `VALUES`): a still-unspecified parameter directly in target-list
        // position resolves against that column's own type, the same way
        // one in a `VALUES` row already does (see `coerce_assign` above) —
        // real Postgres does this too. Since a bare `SELECT $1` alone has
        // no such context, resolving it defaults to `text` (postponing
        // the unknown to the whole query, see `resolve_unknown_output`),
        // which then permanently locks the parameter's type before this
        // function ever sees it — so this has to pre-seed the still-open
        // parameters before `bind_query` runs, not fix them up after.
        if q.with.is_none()
            && let a::SetExpr::Select(sel) = q.body.as_ref()
            && sel.projection.len() == targets.len()
        {
            for (item, (ty, _)) in sel.projection.iter().zip(targets) {
                if let a::SelectItem::UnnamedExpr(a::Expr::Value(vws)) = item
                    && let a::Value::Placeholder(p) = &vws.value
                    && let Some(idx) = p.strip_prefix('$').and_then(|n| n.parse::<usize>().ok())
                    && idx >= 1
                {
                    if self.params.len() < idx {
                        self.params.resize(idx, Type::UNKNOWN);
                    }
                    if self.params[idx - 1].is_unknown() {
                        self.params[idx - 1] = *ty;
                    }
                }
            }
        }
        let (plan, cols_out) = self.bind_query(q)?;
        // Cast the query's columns to the target types.
        let targets_t: Vec<Type> = targets.iter().map(|t| t.0).collect();
        if cols_out.len() == targets.len() {
            for (c, t) in cols_out.iter().zip(&targets_t) {
                if c.ty != *t
                    && casts::cast_context(c.ty, *t).is_none_or(|x| x > CastCtx::Assignment)
                {
                    return Err(PgError::new(
                        code::DATATYPE_MISMATCH,
                        format!(
                            "column \"{}\" is of type {} but expression is of type {}",
                            c.name,
                            t.display(-1),
                            c.ty.display(-1)
                        ),
                    )
                    .hint("You will need to rewrite or cast the expression."));
                }
            }
            let plan = self.cast_query_columns(plan, &cols_out, &targets_t)?;
            return Ok((plan, cols_out));
        }
        Ok((plan, cols_out))
    }

    fn coerce_assign(
        &mut self,
        te: TE,
        ty: Type,
        typmod: i32,
        col: &str,
        table: &str,
    ) -> PgResult<Expr> {
        let from = te.ty;
        self.coerce(te, ty, typmod, CastCtx::Assignment, col).map_err(|e| {
            if e.code == code::CANNOT_COERCE || e.code == code::DATATYPE_MISMATCH {
                PgError::new(
                    code::DATATYPE_MISMATCH,
                    format!(
                        "column \"{col}\" is of type {} but expression is of type {}",
                        ty.display(-1),
                        from.display(-1)
                    ),
                )
                .hint("You will need to rewrite or cast the expression.")
                .table("public", table)
            } else {
                e
            }
        })
    }

    fn column_defaults(&mut self, table: &Table) -> PgResult<Vec<Option<Expr>>> {
        let mut out = vec![];
        for c in &table.columns {
            let src = c.default.clone().or_else(|| {
                c.identity.map(|(_, seq)| {
                    let text = self.db.sequences.get(&seq).map_or_else(String::new, |s| {
                        self.db.regclass_text(s.schema, &s.name, &self.sess.search_path)
                    });
                    format!("nextval('{text}'::regclass)")
                })
            });
            out.push(match src {
                None => None,
                Some(sql) => {
                    let te = self.bind_sql_expr(&sql)?;
                    Some(self.coerce(te, c.ty, c.typmod, CastCtx::Assignment, &c.name)?)
                }
            });
        }
        Ok(out)
    }

    /// Binds an expression against a table's columns (check constraints,
    /// generated columns, index expressions).
    pub fn bind_table_expr(&mut self, t: &Table, sql: &str) -> PgResult<Expr> {
        let scope = table_scope(t, t.oid, &t.name, 0);
        self.scopes.push(scope);
        self.frames.push(AggFrame { forbid: Some("a constraint"), ..Default::default() });
        let r = self.bind_sql_expr(sql);
        self.frames.pop();
        self.scopes.pop();
        Ok(r?.e)
    }

    /// Binds a generated column's expression, coerced to the column type.
    pub fn bind_generated(
        &mut self,
        t: &Table,
        sql: &str,
        ty: Type,
        typmod: i32,
    ) -> PgResult<Expr> {
        let scope = table_scope(t, t.oid, &t.name, 0);
        self.scopes.push(scope);
        self.frames.push(AggFrame { forbid: Some("a generated column"), ..Default::default() });
        let r = self.bind_sql_expr(sql);
        self.frames.pop();
        self.scopes.pop();
        let te = r?;
        self.coerce(te, ty, typmod, CastCtx::Assignment, "generated column")
    }

    /// Binds a standalone SQL expression (defaults, check constraints).
    pub fn bind_sql_expr(&mut self, sql: &str) -> PgResult<TE> {
        let expr = super::parse_expr(sql)?;
        self.bind_expr(&expr)
    }

    fn bind_on_conflict(
        &mut self,
        oc: &a::OnConflict,
        table: &Table,
        oid: u32,
    ) -> PgResult<OnConflict> {
        let target = match &oc.conflict_target {
            None => None,
            Some(a::ConflictTarget::Columns(cols)) => {
                let mut idx = vec![];
                for c in cols {
                    let n = ident(c);
                    idx.push(table.col_index(&n).ok_or_else(|| {
                        PgError::new(
                            code::UNDEFINED_COLUMN,
                            format!("column \"{n}\" does not exist"),
                        )
                    })?);
                }
                // The columns must be exactly some unique index's.
                let mut want = idx.clone();
                want.sort_unstable();
                let matches = table
                    .constraints
                    .iter()
                    .filter(|c| {
                        matches!(
                            c.kind,
                            super::catalog::ConstraintKind::PrimaryKey
                                | super::catalog::ConstraintKind::Unique
                        )
                    })
                    .map(|c| c.cols.clone())
                    .chain(
                        table
                            .indexes
                            .iter()
                            .filter(|i| i.unique && i.cols.iter().all(Option::is_some))
                            .map(|i| i.cols.iter().flatten().copied().collect()),
                    )
                    .any(|mut c: Vec<usize>| {
                        c.sort_unstable();
                        c == want
                    });
                if !matches {
                    return Err(PgError::new(
                        code::INVALID_COLUMN_REFERENCE,
                        "there is no unique or exclusion constraint matching the ON CONFLICT specification",
                    ));
                }
                Some(idx)
            }
            Some(a::ConflictTarget::OnConstraint(name)) => {
                let n = object_name(name).pop().unwrap_or_default();
                let c = table.constraints.iter().find(|c| c.name == n).ok_or_else(|| {
                    PgError::new(
                        code::UNDEFINED_OBJECT,
                        format!("constraint \"{n}\" for table \"{}\" does not exist", table.name),
                    )
                })?;
                Some(c.cols.clone())
            }
        };
        let action = match &oc.action {
            a::OnConflictAction::DoNothing => ConflictAction::Nothing,
            a::OnConflictAction::DoUpdate(du) => {
                // Scope: the existing row, then EXCLUDED.
                let mut scope = Scope::default();
                for (i, c) in table.live_columns() {
                    scope.cols.push(SCol {
                        rel: Some(table.name.clone()),
                        name: c.name.clone(),
                        ty: c.ty,
                        typmod: c.typmod,
                        idx: i,
                        table_oid: oid,
                        attnum: i as i16 + 1,
                        hidden: false,
                        system: false,
                        rec: None,
                    });
                }
                let width = table.columns.len();
                for (i, c) in table.live_columns() {
                    scope.cols.push(SCol {
                        rel: Some("excluded".into()),
                        name: c.name.clone(),
                        ty: c.ty,
                        typmod: c.typmod,
                        idx: width + i,
                        table_oid: 0,
                        attnum: i as i16 + 1,
                        hidden: true,
                        system: false,
                        rec: None,
                    });
                }
                scope.rels.push(table.name.clone());
                scope.rels.push("excluded".into());
                self.scopes.push(scope);
                self.frames
                    .push(AggFrame { forbid: Some("ON CONFLICT DO UPDATE"), ..Default::default() });
                let r = (|| -> PgResult<ConflictAction> {
                    let mut sets = vec![];
                    for asg in &du.assignments {
                        let (idx, expr) = self.bind_assignment(asg, table)?;
                        sets.push((idx, expr));
                    }
                    let filter = match &du.selection {
                        Some(f) => {
                            let te = self.bind_expr(f)?;
                            Some(self.bool_expr(te, "WHERE")?)
                        }
                        None => None,
                    };
                    Ok(ConflictAction::Update { sets, filter })
                })();
                self.frames.pop();
                self.scopes.pop();
                r?
            }
        };
        Ok(OnConflict { target, action })
    }

    fn bind_assignment(&mut self, asg: &a::Assignment, table: &Table) -> PgResult<(usize, Expr)> {
        let a::AssignmentTarget::ColumnName(name) = &asg.target else {
            return Err(unsupported("multi-column assignment"));
        };
        let parts = object_name(name);
        let col = parts.last().cloned().unwrap_or_default();
        let idx = table.col_index(&col).ok_or_else(|| {
            PgError::new(
                code::UNDEFINED_COLUMN,
                format!("column \"{col}\" of relation \"{}\" does not exist", table.name),
            )
        })?;
        if matches!(&asg.value, a::Expr::Identifier(id) if ident(id) == "default") {
            return Ok((idx, Expr::Default(idx)));
        }
        if let Some((true, _)) = table.columns[idx].identity {
            return Err(PgError::new(
                code::GENERATED_ALWAYS,
                format!("column \"{col}\" can only be updated to DEFAULT"),
            )
            .detail(format!(
                "Column \"{col}\" is an identity column defined as GENERATED ALWAYS."
            )));
        }
        let te = self.bind_expr(&asg.value)?;
        let c = &table.columns[idx];
        let e = self.coerce_assign(te, c.ty, c.typmod, &c.name, &table.name)?;
        Ok((idx, e))
    }

    fn bind_returning(
        &mut self,
        returning: &Option<Vec<a::SelectItem>>,
        table: &Table,
        oid: u32,
    ) -> PgResult<(Vec<Expr>, Vec<OutCol>)> {
        let Some(items) = returning else { return Ok((vec![], vec![])) };
        let mut scope = Scope::default();
        for (i, c) in table.live_columns() {
            scope.cols.push(SCol {
                rel: Some(table.name.clone()),
                name: c.name.clone(),
                ty: c.ty,
                typmod: c.typmod,
                idx: i,
                table_oid: oid,
                attnum: i as i16 + 1,
                hidden: false,
                system: false,
                rec: None,
            });
        }
        // System columns follow the table's (the DML appends their values:
        // `RETURNING xmax = 0` tells an upsert's insert from its update).
        push_system_cols(&mut scope, &table.name, oid, table.columns.len());
        scope.rels.push(table.name.clone());
        self.scopes.push(scope);
        self.frames.push(AggFrame { forbid: Some("RETURNING"), ..Default::default() });
        let r = self.bind_projection(items);
        self.frames.pop();
        self.scopes.pop();
        let (proj, cols, _) = r?;
        Ok((proj.into_iter().map(|p| p.e).collect(), cols))
    }

    fn bind_update(&mut self, up: &a::Update) -> PgResult<Planned> {
        let a::TableFactor::Table { name, alias, .. } = &up.table.relation else {
            return Err(unsupported("UPDATE of this FROM item"));
        };
        if !up.table.joins.is_empty() {
            return Err(unsupported("UPDATE with joins in the target"));
        }
        let parts = object_name(name);
        let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        let tname = parts.last().cloned().unwrap_or_default();
        let oid = self.lookup_table_oid(schema.as_deref(), &tname)?;
        let table = self.db.table(oid).unwrap();
        let rel = alias.as_ref().map(|al| ident(&al.name)).unwrap_or_else(|| tname.clone());
        let mut scope = table_scope(table, oid, &rel, 0);
        // FROM
        let from_items = match &up.from {
            Some(a::UpdateTableFromKind::BeforeSet(f))
            | Some(a::UpdateTableFromKind::AfterSet(f)) => f.clone(),
            None => vec![],
        };
        let mut from_plan = None;
        if !from_items.is_empty() {
            let base = scope.width();
            let (f, fscope) = self.bind_from_at(&from_items, base)?;
            merge_scopes(&mut scope, fscope)?;
            from_plan = Some(f);
        }
        self.scopes.push(scope);
        self.frames.push(AggFrame { forbid: Some("UPDATE"), ..Default::default() });
        let r = (|| -> PgResult<Assignments> {
            let mut sets = vec![];
            for asg in &up.assignments {
                sets.push(self.bind_assignment(asg, table)?);
            }
            let filter = match &up.selection {
                Some(f) => {
                    let te = self.bind_expr(f)?;
                    Some(self.bool_expr(te, "WHERE")?)
                }
                None => None,
            };
            Ok((sets, filter))
        })();
        self.frames.pop();
        self.scopes.pop();
        let (sets, filter) = r?;
        let defaults = self.column_defaults(table)?;
        let (returning, rcols) = self.bind_returning(&up.returning, table, oid)?;
        Ok(Planned {
            query: Query::Dml(Box::new(Dml::Update {
                table: oid,
                from: from_plan,
                filter,
                sets,
                defaults,
                returning,
            })),
            cols: rcols,
            tag: "UPDATE",
            cte_slots: self.cte_slots,
            returns_rows: up.returning.is_some(),
        })
    }

    fn bind_from_at(
        &mut self,
        items: &[a::TableWithJoins],
        base: usize,
    ) -> PgResult<(From, Scope)> {
        let (mut from, mut scope) = self.bind_table_with_joins_at(&items[0], base, None)?;
        for item in &items[1..] {
            let (rhs, rscope) =
                self.bind_table_with_joins_at(item, base + scope.width(), Some(&scope))?;
            let left_cols = scope.width();
            let right_cols = rscope.width();
            merge_scopes(&mut scope, rscope)?;
            from = From::Join {
                left: Box::new(from),
                right: Box::new(rhs),
                kind: JoinKind::Cross,
                on: None,
                lateral: is_lateral_item(&item.relation),
                left_cols,
                right_cols,
            };
        }
        Ok((from, scope))
    }

    fn bind_delete(&mut self, del: &a::Delete) -> PgResult<Planned> {
        let tables = match &del.from {
            a::FromTable::WithFromKeyword(t) | a::FromTable::WithoutKeyword(t) => t.clone(),
        };
        if tables.len() != 1 {
            return Err(unsupported("DELETE from multiple tables"));
        }
        let a::TableFactor::Table { name, alias, .. } = &tables[0].relation else {
            return Err(unsupported("DELETE from this FROM item"));
        };
        let parts = object_name(name);
        let schema = (parts.len() > 1).then(|| parts[parts.len() - 2].clone());
        let tname = parts.last().cloned().unwrap_or_default();
        let oid = self.lookup_table_oid(schema.as_deref(), &tname)?;
        let table = self.db.table(oid).unwrap();
        let rel = alias.as_ref().map(|al| ident(&al.name)).unwrap_or_else(|| tname.clone());
        let mut scope = table_scope(table, oid, &rel, 0);
        let mut using_plan = None;
        if let Some(using) = &del.using
            && !using.is_empty()
        {
            let base = scope.width();
            let (f, fscope) = self.bind_from_at(using, base)?;
            merge_scopes(&mut scope, fscope)?;
            using_plan = Some(f);
        }
        self.scopes.push(scope);
        self.frames.push(AggFrame { forbid: Some("DELETE"), ..Default::default() });
        let filter = match &del.selection {
            Some(f) => {
                let te = self.bind_expr(f);
                match te {
                    Ok(te) => Some(self.bool_expr(te, "WHERE")),
                    Err(e) => Some(Err(e)),
                }
            }
            None => None,
        };
        self.frames.pop();
        self.scopes.pop();
        let filter = filter.transpose()?;
        let (returning, rcols) = self.bind_returning(&del.returning, table, oid)?;
        Ok(Planned {
            query: Query::Dml(Box::new(Dml::Delete {
                table: oid,
                using: using_plan,
                filter,
                returning,
            })),
            cols: rcols,
            tag: "DELETE",
            cte_slots: self.cte_slots,
            returns_rows: del.returning.is_some(),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers

/// Bound SET assignments and an optional WHERE clause.
type Assignments = (Vec<(usize, Expr)>, Option<Expr>);

/// Parameter names of the functions that accept named arguments.
fn param_names(func: &str) -> Option<&'static [&'static str]> {
    match func {
        "make_interval" => Some(&["years", "months", "weeks", "days", "hours", "mins", "secs"]),
        "make_timestamp" => Some(&["year", "month", "mday", "hour", "min", "sec"]),
        "make_timestamptz" => Some(&["year", "month", "mday", "hour", "min", "sec", "timezone"]),
        "make_date" => Some(&["year", "month", "mday"]),
        "make_time" => Some(&["hour", "min", "sec"]),
        _ => None,
    }
}

/// Expands `f(a, name => b)` into positional arguments.
fn place_named_args(
    func: &str,
    positional: Vec<a::Expr>,
    named: Vec<(String, a::Expr)>,
) -> PgResult<Vec<a::Expr>> {
    let Some(names) = param_names(func) else {
        return Err(PgError::new(
            code::UNDEFINED_FUNCTION,
            format!("function {func} does not exist"),
        )
        .hint("No function matches the given name and argument types. You might need to add explicit type casts."));
    };
    let zero = a::Expr::Value(a::Value::Number("0".into(), false).into());
    let mut out: Vec<a::Expr> = names.iter().map(|_| zero.clone()).collect();
    for (i, e) in positional.into_iter().enumerate() {
        if i >= out.len() {
            return Err(PgError::new(code::SYNTAX_ERROR, format!("too many arguments for {func}")));
        }
        out[i] = e;
    }
    for (n, e) in named {
        let idx = names.iter().position(|p| *p == n).ok_or_else(|| {
            PgError::new(
                code::UNDEFINED_FUNCTION,
                format!("function {func} has no parameter \"{n}\""),
            )
        })?;
        out[idx] = e;
    }
    Ok(out)
}

/// Rewrites unknown-typed output columns to text, as Postgres does when a
/// query's result type is finalized.
fn resolve_unknown_output(q: &mut Query, cols: &mut [OutCol]) {
    for (i, c) in cols.iter_mut().enumerate() {
        if c.ty.is_unknown() {
            c.ty = Type::TEXT;
            force_text(q, i);
        }
    }
}

fn force_text(q: &mut Query, col: usize) {
    match q {
        Query::Select(s) => {
            if let Some(e) = s.proj.get_mut(col) {
                let old = std::mem::replace(e, Expr::Const(Value::Null));
                *e = to_text_expr(old);
            }
        }
        Query::Values { rows, .. } => {
            for r in rows {
                if let Some(e) = r.get_mut(col) {
                    let old = std::mem::replace(e, Expr::Const(Value::Null));
                    *e = to_text_expr(old);
                }
            }
        }
        Query::SetOp { left, right, .. } => {
            force_text(left, col);
            force_text(right, col);
        }
        Query::With { body, .. } => force_text(body, col),
        Query::Recursive { seed, step, .. } => {
            force_text(seed, col);
            force_text(step, col);
        }
        Query::Dml(_) => {}
    }
}

fn to_text_expr(e: Expr) -> Expr {
    match e {
        Expr::Const(Value::Text(s)) => Expr::Const(Value::Text(s)),
        Expr::Const(Value::Null) => Expr::Const(Value::Null),
        other => Expr::Cast {
            expr: Box::new(other),
            from: Type::UNKNOWN,
            to: Type::TEXT,
            typmod: -1,
            explicit: false,
        },
    }
}

/// A bound expression with its type.
#[derive(Clone, Debug)]
pub struct TE {
    pub e: Expr,
    pub ty: Type,
    pub typmod: i32,
    pub table_oid: u32,
    pub attnum: i16,
    pub rec: Option<Vec<(String, Type)>>,
    /// An explicit `COLLATE` (a column's own collation is looked up from
    /// `table_oid`/`attnum`).
    pub collation: Option<String>,
}

impl TE {
    pub fn new(e: Expr, ty: Type) -> TE {
        TE { e, ty, typmod: -1, table_oid: 0, attnum: 0, rec: None, collation: None }
    }
    fn with_typmod(mut self, m: i32) -> TE {
        self.typmod = m;
        self
    }
}

pub fn table_out_cols(t: &Table) -> Vec<OutCol> {
    t.live_columns()
        .map(|(i, c)| OutCol {
            name: c.name.clone(),
            ty: c.ty,
            typmod: c.typmod,
            table_oid: t.oid,
            attnum: i as i16 + 1,
            rec: None,
        })
        .collect()
}

/// `ctid`, `xmin`, `cmin`, `xmax`, `cmax`, `tableoid`: Postgres's per-row
/// system columns, in the same order as their real (negative) attnums.
/// noida-db has no MVCC, so only `ctid` (scan position) and `tableoid`
/// reflect real per-row state; the transaction/command ids are fixed
/// placeholders (see docs/LIMITATIONS.md) so a client that blindly selects
/// them (some ORMs' optimistic-locking or catalog-introspection code does)
/// gets a plausible value instead of a hard "column does not exist" error.
pub(super) const SYSTEM_COLS: &[(&str, Type, i16)] = &[
    ("ctid", Type::TID, -1),
    ("xmin", Type::XID, -2),
    ("cmin", Type::CID, -3),
    ("xmax", Type::XID, -4),
    ("cmax", Type::CID, -5),
    ("tableoid", Type::OID, -6),
];

fn push_system_cols(scope: &mut Scope, rel: &str, oid: u32, base: usize) {
    for (i, (name, ty, attnum)) in SYSTEM_COLS.iter().enumerate() {
        scope.cols.push(SCol {
            rel: Some(rel.to_string()),
            name: (*name).to_string(),
            ty: *ty,
            typmod: -1,
            idx: base + i,
            table_oid: oid,
            attnum: *attnum,
            hidden: true,
            system: true,
            rec: None,
        });
    }
}

fn table_scope(t: &Table, oid: u32, rel: &str, base: usize) -> Scope {
    let mut scope = Scope::default();
    for (i, c) in t.live_columns() {
        scope.cols.push(SCol {
            rel: Some(rel.to_string()),
            name: c.name.clone(),
            ty: c.ty,
            typmod: c.typmod,
            idx: base + i,
            table_oid: oid,
            attnum: i as i16 + 1,
            hidden: false,
            system: false,
            rec: None,
        });
    }
    push_system_cols(&mut scope, rel, oid, base + t.columns.len());
    scope.rels.push(rel.to_string());
    scope
}

fn merge_scopes(into: &mut Scope, other: Scope) -> PgResult<()> {
    for r in &other.rels {
        if into.rels.contains(r) {
            return Err(PgError::new(
                code::DUPLICATE_ALIAS,
                format!("table name \"{r}\" specified more than once"),
            ));
        }
    }
    into.cols.extend(other.cols);
    into.merged.extend(other.merged);
    into.rels.extend(other.rels);
    Ok(())
}

fn apply_alias_columns(
    cols: &mut [OutCol],
    names: &[a::TableAliasColumnDef],
    rel: &str,
) -> PgResult<()> {
    if names.is_empty() {
        return Ok(());
    }
    if names.len() > cols.len() {
        return Err(PgError::new(
            code::SYNTAX_ERROR,
            format!(
                "table \"{rel}\" has {} columns available but {} columns specified",
                cols.len(),
                names.len()
            ),
        ));
    }
    for (c, n) in cols.iter_mut().zip(names) {
        c.name = ident(&n.name);
    }
    Ok(())
}

fn rename_cols(cols: &mut [OutCol], names: &[a::TableAliasColumnDef], rel: &str) -> PgResult<()> {
    apply_alias_columns(cols, names, rel)
}

/// Flattens top-level (and nested) `AND`s into a flat conjunct list, the
/// inverse of [`and_all`].
fn flatten_and(e: Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::And(items) => items.into_iter().for_each(|i| flatten_and(i, out)),
        other => out.push(other),
    }
}

/// The highest column index a bound expression references in the current
/// row, ignoring outer-query references (which are always "in scope").
/// `None` means the expression references no row column at all.
fn max_col(e: &Expr) -> Option<usize> {
    let mut m = match e {
        Expr::Col(i) => Some(*i),
        _ => None,
    };
    let mut e = e.clone();
    e.children_mut(&mut |c| {
        if let Some(cm) = max_col(c) {
            m = Some(m.map_or(cm, |x| x.max(cm)));
        }
    });
    m
}

/// A comma-separated `FROM a, b, c` binds to a left-deep chain of `Cross`
/// joins with no `on`, and an explicit `a JOIN b ON ... JOIN c ON ...`
/// binds to the same left-deep shape with `Inner` joins that already have
/// one. Either way, without this a WHERE-clause condition on an early
/// table only applies after every later join has already run against the
/// *whole* earlier result — many ORMs' catalog-introspection queries send
/// exactly this shape (a long explicit-join chain filtered down to one
/// specific row only in WHERE), and it blows up memory/time once the
/// tables involved aren't tiny. Real Postgres has no such distinction
/// (comma joins, `JOIN...ON` and WHERE conditions on an inner join are
/// all equivalent), so it's correct to push a WHERE conjunct down onto
/// (or, for an `Inner`/`Cross` join that already has one, AND it onto)
/// whichever join makes all of its columns available first. `Left`/
/// `Right`/`Full` joins are never touched (doing so would change which
/// rows get null-padded), but a chain's own accumulation is always
/// through `left` — see `bind_table_with_joins_at` — so descending only
/// through `left` still reaches every `Inner`/`Cross` node nested inside
/// an outer join's left-hand side. Only ever descends through `left` on
/// the other axis too: a `right` item can itself be an arbitrary join
/// subtree at some other column offset (a later comma-joined item), which
/// this deliberately leaves untouched.
fn push_cross_predicates(from: &mut From, remaining: &mut Vec<Expr>) {
    let From::Join { left, kind, on, left_cols, right_cols, .. } = from else { return };
    push_cross_predicates(left, remaining);
    if !matches!(kind, JoinKind::Cross | JoinKind::Inner) || remaining.is_empty() {
        return;
    }
    let width = *left_cols + *right_cols;
    let mut mine = vec![];
    let mut i = 0;
    while i < remaining.len() {
        if max_col(&remaining[i]).is_none_or(|m| m < width) {
            mine.push(remaining.remove(i));
        } else {
            i += 1;
        }
    }
    if mine.is_empty() {
        return;
    }
    let extra = and_all(mine);
    *on = Some(match on.take() {
        Some(existing) => Expr::And(vec![existing, extra]),
        None => extra,
    });
}

fn and_all(mut conds: Vec<Expr>) -> Expr {
    match conds.len() {
        0 => Expr::Const(Value::Bool(true)),
        1 => conds.pop().unwrap(),
        _ => Expr::And(conds),
    }
}

fn bool_check(e: Expr, what: &str) -> PgResult<Expr> {
    let _ = what;
    Ok(e)
}

fn missing_from(rel: &str) -> PgError {
    PgError::new(code::UNDEFINED_TABLE, format!("missing FROM-clause entry for table \"{rel}\""))
}

fn no_operator(op: &str, l: Type, r: Type) -> PgError {
    PgError::new(
        code::UNDEFINED_FUNCTION,
        format!("operator does not exist: {} {op} {}", l.display(-1), r.display(-1)),
    )
    .hint("No operator matches the given name and argument types. You might need to add explicit type casts.")
}

fn cannot_coerce(from: Type, to: Type, ctx: CastCtx, what: &str) -> PgError {
    if ctx == CastCtx::Explicit {
        return casts::cannot_cast(from, to);
    }
    if to.base == Base::Bool {
        return PgError::new(
            code::DATATYPE_MISMATCH,
            format!("argument of {what} must be type boolean, not type {}", from.display(-1)),
        );
    }
    PgError::new(
        code::DATATYPE_MISMATCH,
        format!(
            "{what} expression is of type {} but should be {}",
            from.display(-1),
            to.display(-1)
        ),
    )
}

/// Numeric/type promotion for CASE, UNION, comparisons.
fn promote(a: Type, b: Type) -> Option<Type> {
    if a == b {
        return Some(a);
    }
    if a.array != b.array {
        return None;
    }
    if a.array {
        return promote(a.elem(), b.elem()).map(|t| t.to_array());
    }
    let implicit = |from: Type, to: Type| casts::cast_context(from, to) == Some(CastCtx::Implicit);
    if a.category() != b.category() {
        // Types from different categories still match when one converts to
        // the other implicitly, or when both convert to text ("char" = varchar).
        if implicit(a, b) && !implicit(b, a) {
            return Some(b);
        }
        if implicit(b, a) && !implicit(a, b) {
            return Some(a);
        }
        if implicit(a, Type::TEXT) && implicit(b, Type::TEXT) {
            return Some(Type::TEXT);
        }
        return None;
    }
    if implicit(a, b) && !implicit(b, a) {
        return Some(b);
    }
    if implicit(b, a) && !implicit(a, b) {
        return Some(a);
    }
    if implicit(a, b) && implicit(b, a) {
        // Both ways: pick the preferred type.
        let pa = a.base.info().is_some_and(|i| i.preferred);
        let pb = b.base.info().is_some_and(|i| i.preferred);
        return Some(if pb && !pa { b } else { a });
    }
    // Numeric category: promote to the widest.
    if a.is_numeric() && b.is_numeric() {
        let rank = |t: Type| match t.base {
            Base::Int2 => 1,
            Base::Int4 => 2,
            Base::Int8 => 3,
            Base::Numeric => 4,
            Base::Float4 => 5,
            _ => 6,
        };
        return Some(if rank(a) >= rank(b) { a } else { b });
    }
    if a.is_string() || b.is_string() {
        return Some(Type::TEXT);
    }
    None
}

/// The type an unknown literal takes from the other operand.
fn guess_unknown(other: Type, op: &str) -> Type {
    if other.is_unknown() {
        return Type::NUMERIC;
    }
    // There is no timestamp + timestamp: `ts + NULL` / `ts + '1 hour'`
    // adds an interval.
    if op == "+"
        && !other.array
        && matches!(other.base, Base::Timestamp | Base::Timestamptz | Base::Time)
    {
        return Type::INTERVAL;
    }
    other
}

/// Operator names noida-db implements, for `OPERATOR(schema.op)` syntax.
fn known_operator(op: &str) -> Option<&'static str> {
    const OPS: &[&str] = &[
        "+", "-", "*", "/", "%", "^", "||", "&", "|", "#", "<<", ">>", "->", "->>", "#>", "#>>",
        "@>", "<@", "?", "?|", "?&", "#-", "&&", "~~", "!~~", "~~*", "!~~*", "~", "!~", "~*",
        "!~*", "^@", "<<=", ">>=",
    ];
    OPS.iter().find(|o| **o == op).copied()
}

fn cmp_op(op: &a::BinaryOperator) -> Option<CmpOp> {
    use a::BinaryOperator as B;
    Some(match op {
        B::Eq => CmpOp::Eq,
        B::NotEq => CmpOp::Ne,
        B::Lt => CmpOp::Lt,
        B::LtEq => CmpOp::Le,
        B::Gt => CmpOp::Gt,
        B::GtEq => CmpOp::Ge,
        _ => return None,
    })
}

fn join_kind(op: &a::JoinOperator) -> PgResult<(JoinKind, Option<a::JoinConstraint>)> {
    use a::JoinOperator as J;
    Ok(match op {
        J::Join(c) | J::Inner(c) => (JoinKind::Inner, Some(c.clone())),
        J::Left(c) | J::LeftOuter(c) => (JoinKind::Left, Some(c.clone())),
        J::Right(c) | J::RightOuter(c) => (JoinKind::Right, Some(c.clone())),
        J::FullOuter(c) => (JoinKind::Full, Some(c.clone())),
        J::CrossJoin(_) => (JoinKind::Cross, None),
        other => return Err(unsupported(&format!("{other:?} join"))),
    })
}

fn field_unit(f: &a::DateTimeField) -> String {
    let s = format!("{f}").to_lowercase();
    match s.as_str() {
        "dayofweek" => "dow".into(),
        "dayofyear" => "doy".into(),
        "isoweek" => "week".into(),
        "timezoneabbr" => "timezone".into(),
        other => other.to_string(),
    }
}

fn strip_nested(e: &a::Expr) -> &a::Expr {
    match e {
        a::Expr::Nested(x) => strip_nested(x),
        other => other,
    }
}

/// Rewrites Col references to Outer references `depth` levels up.
fn outerize(mut e: Expr, depth: usize) -> Expr {
    match &mut e {
        Expr::Col(i) => return Expr::Outer(depth, *i),
        other => other.children_mut(&mut |c| *c = outerize(c.clone(), depth)),
    }
    e
}

/// Whether a FROM item may refer to the items before it.
fn is_lateral_item(f: &a::TableFactor) -> bool {
    matches!(
        f,
        a::TableFactor::Derived { lateral: true, .. }
            | a::TableFactor::Function { lateral: true, .. }
            | a::TableFactor::UNNEST { .. }
    ) || matches!(f, a::TableFactor::Table { args: Some(_), .. })
}

/// Renumbers columns so that the ones at `base` and beyond start at zero.
fn rebase_cols(e: &mut Expr, base: usize) {
    if let Expr::Col(i) = e {
        *i = i.saturating_sub(base);
        return;
    }
    e.children_mut(&mut |c| rebase_cols(c, base));
}

fn shift_winrefs(e: &mut Expr, base: usize) {
    if let Expr::WinRef(i) = e {
        *e = Expr::Col(base + *i);
        return;
    }
    e.children_mut(&mut |c| shift_winrefs(c, base));
}

fn expr_has_srf(e: &Expr) -> bool {
    e.contains(&|x| matches!(x, Expr::Call { name, .. } if sigs::kind_of(name) == Some(Kind::Srf)))
}

/// Calls `f` on every expression of `q` (and its subqueries), mutably,
/// with how many subquery levels below `q` it sits (`level`).
fn map_query_exprs(q: &mut Query, level: usize, f: &mut dyn FnMut(&mut Expr, usize)) {
    fn walk(e: &mut Expr, level: usize, f: &mut dyn FnMut(&mut Expr, usize)) {
        f(e, level);
        match e {
            Expr::Sub { query, .. } => map_query_exprs(query, level + 1, f),
            Expr::InSub { query, left, .. } => {
                for l in left {
                    walk(l, level, f);
                }
                map_query_exprs(query, level + 1, f);
            }
            _ => e.children_mut(&mut |c| walk(c, level, f)),
        }
    }
    fn walk_from(fr: &mut From, level: usize, f: &mut dyn FnMut(&mut Expr, usize)) {
        match fr {
            From::Sub(q) => map_query_exprs(q, level + 1, f),
            From::Func { args, .. } => args.iter_mut().for_each(|a| walk(a, level, f)),
            From::Join { left, right, on, .. } => {
                walk_from(left, level, f);
                walk_from(right, level, f);
                if let Some(o) = on {
                    walk(o, level, f);
                }
            }
            _ => {}
        }
    }
    match q {
        Query::Select(s) => {
            walk_from(&mut s.from, level, f);
            let s = &mut **s;
            for e in s
                .proj
                .iter_mut()
                .chain(s.filter.iter_mut())
                .chain(s.having.iter_mut())
                .chain(s.limit.iter_mut())
                .chain(s.offset.iter_mut())
            {
                walk(e, level, f);
            }
            for e in s.group.iter_mut().flatten() {
                walk(e, level, f);
            }
            for agg in &mut s.aggs {
                for e in agg.args.iter_mut().chain(agg.filter.iter_mut()) {
                    walk(e, level, f);
                }
                for (e, ..) in &mut agg.order {
                    walk(e, level, f);
                }
            }
            for w in &mut s.windows {
                for e in w.args.iter_mut().chain(w.partition.iter_mut()) {
                    walk(e, level, f);
                }
                for (e, ..) in &mut w.order {
                    walk(e, level, f);
                }
            }
        }
        Query::Values { rows, .. } => {
            for e in rows.iter_mut().flatten() {
                walk(e, level, f);
            }
        }
        Query::SetOp { left, right, .. } => {
            map_query_exprs(left, level, f);
            map_query_exprs(right, level, f);
        }
        Query::With { ctes, body } => {
            for c in ctes {
                map_query_exprs(&mut c.query, level, f);
            }
            map_query_exprs(body, level, f);
        }
        Query::Recursive { seed, step, .. } => {
            map_query_exprs(seed, level, f);
            map_query_exprs(step, level, f);
        }
        _ => {}
    }
}

fn attach_tail(q: Query, order: Vec<SortKey>, limit: Option<Expr>, offset: Option<Expr>) -> Query {
    match q {
        Query::SetOp { op, all, left, right, .. } => {
            Query::SetOp { op, all, left, right, order, limit, offset }
        }
        Query::Values { rows, .. } => Query::Values { rows, order, limit, offset },
        other => other,
    }
}

/// Whether a CTE query mentions its own name (so it is recursive).
fn query_references(q: &a::Query, name: &str) -> bool {
    let sql = q.to_string().to_lowercase();
    sql.split(|c: char| !c.is_alphanumeric() && c != '_').any(|w| w == name)
}

fn first_words(s: &str) -> String {
    s.split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

/// How many columns an INSERT source produces, when that's knowable from
/// the syntax alone: a VALUES list's row width, or a SELECT list without
/// wildcards.
fn source_width(q: &a::Query) -> Option<usize> {
    match q.body.as_ref() {
        a::SetExpr::Values(v) => v.rows.first().map(|r| r.len()),
        a::SetExpr::Select(sel)
            if sel.projection.iter().all(|p| {
                matches!(p, a::SelectItem::UnnamedExpr(_) | a::SelectItem::ExprWithAlias { .. })
            }) =>
        {
            Some(sel.projection.len())
        }
        _ => None,
    }
}

/// An enum-typed expression as its label's sort position (enums order by
/// declaration, not alphabetically); anything else unchanged.
fn enum_order(e: Expr, ty: Type) -> Expr {
    match (ty.base, ty.array) {
        (Base::Enum(oid), false) => Expr::Call {
            name: "__enum_sortorder",
            args: vec![Expr::Const(Value::Int(oid as i64)), e],
            ty: Type::FLOAT8,
            arg_tys: vec![Type::INT4, ty],
        },
        _ => e,
    }
}

/// `lower(e)`, for comparing under a case-insensitive collation.
fn fold_case(e: Expr) -> Expr {
    Expr::Call { name: "lower", args: vec![e], ty: Type::TEXT, arg_tys: vec![Type::TEXT] }
}

/// Built-in functions that are VOLATILE or STABLE: never folded at plan
/// time, and not allowed in index expressions.
pub const NOT_IMMUTABLE: &[&str] = &[
    "now",
    "clock_timestamp",
    "statement_timestamp",
    "transaction_timestamp",
    "random",
    "gen_random_uuid",
    "uuid_generate_v4",
    "nextval",
    "currval",
    "lastval",
    "setval",
    "current_date",
    "current_time",
    "localtime",
    "localtimestamp",
    "current_setting",
    "set_config",
    "timeofday",
    "pg_sleep",
];

/// Whether an index expression uses something not IMMUTABLE: a volatile
/// or stable function, or a timestamptz cast that depends on TimeZone.
pub fn uses_mutable(e: &Expr) -> bool {
    let mut found = false;
    let mut stack = vec![e.clone()];
    while let Some(mut x) = stack.pop() {
        match &x {
            Expr::Call { name, .. } if NOT_IMMUTABLE.contains(name) => found = true,
            Expr::Cast { from, to, .. }
                if from.base == Base::Timestamptz
                    && !from.array
                    && matches!(
                        to.base,
                        Base::Date | Base::Timestamp | Base::Time | Base::Text | Base::Varchar
                    ) =>
            {
                found = true
            }
            _ => {}
        }
        if found {
            return true;
        }
        x.children_mut(&mut |c| stack.push(c.clone()));
    }
    false
}
