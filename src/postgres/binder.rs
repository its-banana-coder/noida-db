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
    pub user: String,
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
    /// USING/NATURAL hides the underlying columns behind a merged one.
    hidden: bool,
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
    pub db: &'a DbState,
    pub sess: &'a SessionInfo,
    /// Parameter types; `unknown` until resolved from context.
    pub params: Vec<Type>,
    scopes: Vec<Scope>,
    ctes: Vec<CteDef>,
    cte_level: usize,
    pub cte_slots: usize,
    frames: Vec<AggFrame>,
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
            db,
            sess,
            params: param_hints.to_vec(),
            scopes: vec![],
            ctes: vec![],
            cte_level: 0,
            cte_slots: 0,
            frames: vec![],
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
                let (query, cols) = self.bind_query(q)?;
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
                    let (query, mut cols) = self.bind_query(&cte.query)?;
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
        let result = self.bind_select_body(sel, q, from);
        self.scopes.pop();
        result
    }

    fn bind_select_body(
        &mut self,
        sel: &a::Select,
        q: &a::Query,
        from: From,
    ) -> PgResult<(Query, Vec<OutCol>)> {
        let input_width = self.scopes.last().unwrap().width();
        // WHERE
        let filter = match &sel.selection {
            Some(e) => {
                self.frames.push(AggFrame { forbid: Some("WHERE"), ..Default::default() });
                let te = self.bind_expr(e);
                self.frames.pop();
                Some(self.bool_expr(te?, "WHERE")?)
            }
            None => None,
        };
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
        for g in &group_exprs {
            if matches!(g, a::Expr::GroupingSets(_) | a::Expr::Cube(_) | a::Expr::Rollup(_)) {
                return Err(unsupported("GROUPING SETS/CUBE/ROLLUP"));
            }
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
        }
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
                if let a::Expr::Identifier(id) = &o.expr {
                    let n = ident(id);
                    if self.lookup_column(&n, None).is_none() {
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
                }
                let te = self.bind_expr(&o.expr)?;
                let existing = proj.iter().position(|p| p.e == te.e);
                order_specs.push((te, desc, nulls_first, existing));
            }
        }
        // DISTINCT
        let mut distinct = Distinct::None;
        let mut distinct_on: Vec<TE> = vec![];
        match &sel.distinct {
            None | Some(a::Distinct::All) => {}
            Some(a::Distinct::Distinct) => distinct = Distinct::All,
            Some(a::Distinct::On(exprs)) => {
                for e in exprs {
                    distinct_on.push(self.bind_expr(e)?);
                }
                distinct = Distinct::On(vec![]);
            }
        }
        let frame = self.frames.last_mut().unwrap();
        let aggs = std::mem::take(&mut frame.aggs);
        let windows = std::mem::take(&mut frame.wins);
        let grouped = !group_keys.is_empty() || !aggs.is_empty();
        // Rewrite everything above the aggregation step.
        let mut having = having_te.map(|te| te.e);
        if grouped {
            let extra = self.expand_group_keys(&mut group_keys);
            let _ = extra;
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
            let col = match existing {
                Some(i) => i,
                None => {
                    if distinct != Distinct::None {
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
            for te in distinct_on {
                let i = match exprs.iter().position(|e| *e == te.e) {
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
            grouping_sets: None,
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
    fn expand_group_keys(&self, _keys: &mut [Expr]) -> usize {
        0
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
                        });
                        proj.push(TE {
                            e: m.expr.clone(),
                            ty: m.ty,
                            typmod: m.typmod,
                            table_oid: 0,
                            attnum: 0,
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
                        });
                        proj.push(TE {
                            e: Expr::Col(c.idx),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
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
                    for c in scope.cols.iter().filter(|c| c.rel.as_deref() == Some(rel.as_str())) {
                        cols.push(OutCol {
                            name: c.name.clone(),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
                        });
                        proj.push(TE {
                            e: Expr::Col(c.idx),
                            ty: c.ty,
                            typmod: c.typmod,
                            table_oid: c.table_oid,
                            attnum: c.attnum,
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
            Expr::Sub { kind, query } => {
                if query_has_outer_ref(&query, 1) {
                    return Err(unsupported("outer references from a subquery in a grouped query"));
                }
                Ok(Expr::Sub { kind, query })
            }
            Expr::InSub { left, op, query, all, negated } => {
                let left = left
                    .into_iter()
                    .map(|l| self.regroup(l, keys, nagg))
                    .collect::<PgResult<Vec<_>>>()?;
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

    // -----------------------------------------------------------------
    // FROM

    fn bind_from(&mut self, items: &[a::TableWithJoins]) -> PgResult<(From, Scope)> {
        if items.is_empty() {
            return Ok((From::One, Scope::default()));
        }
        let (mut from, mut scope) = self.bind_table_with_joins(&items[0])?;
        for item in &items[1..] {
            let (rhs, rscope) = self.bind_table_with_joins_at(item, scope.width())?;
            let left_cols = scope.width();
            let right_cols = rscope.width();
            merge_scopes(&mut scope, rscope)?;
            from = From::Join {
                left: Box::new(from),
                right: Box::new(rhs),
                kind: JoinKind::Cross,
                on: None,
                lateral: false,
                left_cols,
                right_cols,
            };
        }
        Ok((from, scope))
    }

    fn bind_table_with_joins(&mut self, t: &a::TableWithJoins) -> PgResult<(From, Scope)> {
        self.bind_table_with_joins_at(t, 0)
    }

    fn bind_table_with_joins_at(
        &mut self,
        t: &a::TableWithJoins,
        base: usize,
    ) -> PgResult<(From, Scope)> {
        let (mut from, mut scope) = self.bind_factor(&t.relation, base, None)?;
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
            scope = combined;
            from = From::Join {
                left: Box::new(from),
                right: Box::new(rhs),
                kind,
                on,
                lateral: matches!(
                    &join.relation,
                    a::TableFactor::Derived { lateral: true, .. }
                        | a::TableFactor::Function { lateral: true, .. }
                ) || matches!(&join.relation, a::TableFactor::Table { args: Some(_), .. }),
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
                    });
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
                let (query, mut cols) = bound?;
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
                self.bind_table_with_joins_at(table_with_joins, base)
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
        let bound = bound?;
        let arg_tys: Vec<Type> = bound.iter().map(|t| t.ty).collect();
        let r = sigs::resolve(fname, &arg_tys)?;
        let mut arg_exprs = vec![];
        for (te, target) in bound.into_iter().zip(r.arg_tys.iter()) {
            arg_exprs.push(self.coerce(te, *target, -1, CastCtx::Implicit, fname)?);
        }
        let mut cols: Vec<OutCol> = if r.sig.cols.is_empty() {
            vec![OutCol::new(fname.to_string(), r.ret)]
        } else {
            r.sig.cols.iter().map(|(n, t)| OutCol::new(n.to_string(), *t)).collect()
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
            });
        }
        scope.rels.push(rel);
        let ncols = cols.len();
        Ok((
            From::Func {
                name: r.sig.name,
                args: arg_exprs,
                arg_tys: r.arg_tys,
                ncols,
                ordinality,
                lateral: left.is_some(),
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
        let oid = self.lookup_table_oid(schema.as_deref(), &name)?;
        let t = self.db.table(oid).unwrap();
        if t.kind == RelKind::View {
            let sql = t.view_sql.clone().unwrap_or_default();
            let stmts = super::parse_sql(&sql)?;
            let a::Statement::Query(q) = &stmts[0] else {
                return Err(PgError::new(code::INTERNAL_ERROR, "bad view definition"));
            };
            let saved = std::mem::take(&mut self.scopes);
            let bound = self.bind_query(q);
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
                })
                .collect();
            return Ok((From::Sub(Box::new(query)), cols, name));
        }
        let cols = table_out_cols(t);
        Ok((From::Table { oid, ncols: t.columns.len() }, cols, name))
    }

    pub fn lookup_table_oid(&self, schema: Option<&str>, name: &str) -> PgResult<u32> {
        let full = match schema {
            Some(s) => format!("{s}.{name}"),
            None => name.to_string(),
        };
        match schema {
            Some(s) => {
                let sid = self.db.schema_by_name(s).ok_or_else(|| {
                    PgError::new(
                        code::INVALID_SCHEMA_NAME,
                        format!("schema \"{s}\" does not exist"),
                    )
                })?;
                self.db
                    .find_table(sid, name)
                    .map(|t| t.oid)
                    .ok_or_else(|| super::catalog::undefined_table(&full))
            }
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
                    TE { e, ty: m.ty, typmod: m.typmod, table_oid: 0, attnum: 0 },
                ));
            }
            let hits: Vec<&SCol> = scope
                .cols
                .iter()
                .filter(|c| {
                    c.name == name
                        && (rel.is_none() && !c.hidden
                            || rel.is_some_and(|r| c.rel.as_deref() == Some(r)))
                })
                .collect();
            if let Some(c) = hits.first() {
                let e = if depth == 0 { Expr::Col(c.idx) } else { Expr::Outer(depth, c.idx) };
                return Some((
                    depth,
                    TE { e, ty: c.ty, typmod: c.typmod, table_oid: c.table_oid, attnum: c.attnum },
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
            let n = scope.cols.iter().filter(|c| c.name == name && !c.hidden).count();
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
                let xe = self.coerce(x, ty, -1, CastCtx::Implicit, "IN")?;
                let mut list_e = vec![];
                for it in items {
                    list_e.push(self.coerce(it, ty, -1, CastCtx::Implicit, "IN")?);
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
                let (plan, cols) = self.bind_query(q)?;
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
                let elem = if rt.ty.array { rt.ty.elem() } else { rt.ty };
                let ty = self.common_type(&[lt.ty, elem], "ANY", 0)?;
                let le = self.coerce(lt, ty, -1, CastCtx::Implicit, "ANY")?;
                let re = self.coerce(rt, ty.to_array(), -1, CastCtx::Implicit, "ANY")?;
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
            E::Substring { expr, substring_from, substring_for, .. } => {
                let mut args = vec![self.bind_expr(expr)?];
                if let Some(f) = substring_from {
                    args.push(self.bind_expr(f)?);
                }
                if let Some(l) = substring_for {
                    args.push(self.bind_expr(l)?);
                }
                self.call("substring", args)
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
            E::Collate { expr, .. } => self.bind_expr(expr),
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
            scope.cols.iter().filter(|c| c.rel.as_deref() == Some(rel)).collect();
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
            a::Value::EscapedStringLiteral(s) => {
                TE::new(Expr::Const(Value::text(unescape_c(s))), Type::UNKNOWN)
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
        Ok(TE::new(Expr::Case { operand: None, whens: out_whens, else_: else_e }, ty))
    }

    fn bind_access(&mut self, root: &a::Expr, chain: &[a::AccessExpr]) -> PgResult<TE> {
        let mut te = self.bind_expr(root)?;
        for acc in chain {
            match acc {
                a::AccessExpr::Subscript(a::Subscript::Index { index }) => {
                    let idx = self.bind_expr(index)?;
                    let idx = self.coerce(idx, Type::INT4, -1, CastCtx::Assignment, "subscript")?;
                    if !te.ty.array && te.ty.base != Base::Jsonb && te.ty.base != Base::Json {
                        return Err(PgError::new(
                            code::DATATYPE_MISMATCH,
                            format!(
                                "cannot subscript type {} because it does not support subscripting",
                                te.ty.display(-1)
                            ),
                        ));
                    }
                    let ret = if te.ty.array { te.ty.elem() } else { te.ty };
                    te = TE::new(
                        Expr::Call {
                            name: "subscript",
                            args: vec![te.e, idx],
                            ty: ret,
                            arg_tys: vec![te.ty, Type::INT4],
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
                    let _ = field;
                    return Err(unsupported("field selection from a composite value"));
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
        let ty = self
            .common_type(&[l.ty, r.ty], "comparison", 0)
            .map_err(|_| no_operator(op.symbol(), l.ty, r.ty))?;
        let le = self.coerce(l, ty, -1, CastCtx::Implicit, "comparison")?;
        let re = self.coerce(r, ty, -1, CastCtx::Implicit, "comparison")?;
        Ok(Expr::Compare {
            op,
            left: Box::new(le),
            right: Box::new(re),
            bpchar: ty.base == Base::Bpchar,
        })
    }

    fn bind_binary(
        &mut self,
        left: &a::Expr,
        op: &a::BinaryOperator,
        right: &a::Expr,
    ) -> PgResult<TE> {
        use a::BinaryOperator as B;
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
            other => return Err(unsupported(&format!("operator {other}"))),
        };
        self.binop_te(name, l, r)
    }

    /// Resolves an operator's operand and result types.
    fn binop_te(&mut self, op: &'static str, l: TE, r: TE) -> PgResult<TE> {
        let (lt, rt) = (l.ty, r.ty);
        let both_unknown = lt.is_unknown() && rt.is_unknown();
        let (ltarget, rtarget, ret): (Type, Type, Type) = match op {
            "+" | "-" | "*" | "/" | "%" | "^" => {
                if both_unknown {
                    return Err(PgError::new(
                        code::AMBIGUOUS_FUNCTION,
                        format!("operator is not unique: unknown {op} unknown"),
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
                if lt.array || rt.array {
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
                let t = if lt.is_unknown() && rt.is_unknown() {
                    Type::INT4
                } else if lt.is_integer() {
                    lt
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
                if lt.array || rt.array {
                    let elem = if lt.array { lt } else { rt };
                    (elem, elem, Type::BOOL)
                } else {
                    (Type::JSONB, Type::JSONB, Type::BOOL)
                }
            }
            "&&" => {
                let elem = if lt.array { lt } else { rt };
                (elem, elem, Type::BOOL)
            }
            "?" => (Type::JSONB, Type::TEXT, Type::BOOL),
            "?|" | "?&" => (Type::JSONB, Type::array_of(Base::Text), Type::BOOL),
            "#-" => (Type::JSONB, Type::array_of(Base::Text), Type::JSONB),
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
            // ^ is float8-only unless both are numeric-ish.
            if op == "^" {
                return if lt.base == Numeric
                    || rt.base == Numeric
                    || (lt.is_integer() && rt.is_integer() && false)
                {
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
        let a::FunctionArguments::List(list) = &f.args else {
            return Err(unsupported("function with subquery arguments"));
        };
        let distinct = matches!(list.duplicate_treatment, Some(a::DuplicateTreatment::Distinct));
        let mut star = false;
        let mut args = vec![];
        for arg in &list.args {
            match arg {
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(e)) => args.push(e.clone()),
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Wildcard) => star = true,
                a::FunctionArg::Named { name: n, arg: a::FunctionArgExpr::Expr(e), .. } => {
                    let _ = n;
                    args.push(e.clone());
                }
                _ => return Err(unsupported("function argument")),
            }
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
        let is_agg = kind == Some(Kind::Agg) || (star && name == "count");
        let is_win = kind == Some(Kind::Window);
        if (is_agg || is_win) && f.over.is_none() {
            if is_win {
                return Err(PgError::new(
                    code::WINDOWING_ERROR,
                    format!("window function {name} requires an OVER clause"),
                ));
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
            "current_user" | "user" | "session_user" | "current_role" => {
                konst(Value::text(self.sess.user.clone()), Type::NAME)
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
        Ok(TE::new(self.fold_call(e)?, r.ret))
    }

    fn fold_call(&self, e: Expr) -> PgResult<Expr> {
        let Expr::Call { name, args, ty, arg_tys } = &e else { return Ok(e) };
        let volatile = matches!(
            *name,
            "now"
                | "clock_timestamp"
                | "statement_timestamp"
                | "transaction_timestamp"
                | "random"
                | "gen_random_uuid"
                | "uuid_generate_v4"
                | "nextval"
                | "currval"
                | "lastval"
                | "setval"
                | "current_date"
                | "current_time"
                | "localtime"
                | "localtimestamp"
                | "current_setting"
                | "set_config"
                | "timeofday"
                | "pg_sleep"
        );
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
        let call = AggCall {
            name: r.sig.name,
            args: out,
            arg_tys: r.arg_tys.clone(),
            ty: r.ret,
            distinct,
            filter: filter_e,
            order,
            star,
        };
        let frame = self.frames.last_mut().unwrap();
        let idx = match frame.aggs.iter().position(|x| *x == call) {
            Some(i) => i,
            None => {
                frame.aggs.push(call);
                frame.aggs.len() - 1
            }
        };
        Ok(TE::new(Expr::AggRef(idx), r.ret))
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
        let a::WindowType::WindowSpec(spec) = over else {
            return Err(unsupported("named windows"));
        };
        if spec.window_name.is_some() {
            return Err(unsupported("named windows"));
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
        self.frames
            .push(AggFrame { forbid: Some("a window function argument"), ..Default::default() });
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
        self.frames.pop();
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
        let bound = |b: &a::WindowFrameBound, me: &mut Self| -> PgResult<FrameBound> {
            Ok(match b {
                a::WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                a::WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                a::WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
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
        if !rows && !matches!(f.units, a::WindowFrameUnits::Range) {
            return Err(unsupported("GROUPS window frames"));
        }
        let start = bound(&f.start_bound, self)?;
        let end = match &f.end_bound {
            Some(b) => bound(b, self)?,
            None => FrameBound::CurrentRow,
        };
        Ok(Frame { rows, start, end })
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
        if target.base == Base::Any || (target.base == Base::AnyElement && !target.array) {
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
                    let v = types::from_text(s, target, &self.dctx())?;
                    let v = types::apply_typmod(v, target, typmod, ctx == CastCtx::Explicit)?;
                    return Ok(Expr::Const(v));
                }
                Expr::Param(i) => {
                    let i = *i;
                    if self.params[i].is_unknown() {
                        self.params[i] = target;
                        return Ok(Expr::Param(i));
                    }
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
        if let Expr::Const(v) = &te.e {
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
        let _ = te;
        use a::Expr as E;
        match e {
            E::Identifier(id) => ident(id),
            E::CompoundIdentifier(ids) => ids.last().map(ident).unwrap_or_default(),
            E::Nested(x) => self.column_name(x, te),
            E::Collate { expr, .. } => self.column_name(expr, te),
            E::Cast { expr, data_type, .. } => {
                let inner = self.column_name(expr, te);
                if inner == "?column?" {
                    self.data_type(data_type)
                        .map(|(t, _)| t.name())
                        .unwrap_or_else(|_| "?column?".into())
                } else {
                    inner
                }
            }
            E::Function(f) => object_name(&f.name).pop().unwrap_or_default(),
            E::Case { .. } => "case".into(),
            E::Exists { .. } => "exists".into(),
            E::Array(_) => "array".into(),
            E::Tuple(_) => "row".into(),
            E::Value(v) => match &v.value {
                a::Value::Boolean(_) => "bool".into(),
                _ => "?column?".into(),
            },
            E::TypedString(ts) => self
                .data_type(&ts.data_type)
                .map(|(t, _)| t.name())
                .unwrap_or_else(|_| "?column?".into()),
            E::Interval(_) => "interval".into(),
            E::Extract { .. } => "extract".into(),
            E::Substring { .. } => "substring".into(),
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
            E::AtTimeZone { timestamp, .. } => self.column_name(timestamp, te),
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
                overriding_system: false,
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
                    let name =
                        self.db.sequences.get(&seq).map(|s| s.name.clone()).unwrap_or_default();
                    format!("nextval('{name}'::regclass)")
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
            });
        }
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
        let (mut from, mut scope) = self.bind_table_with_joins_at(&items[0], base)?;
        for item in &items[1..] {
            let (rhs, rscope) = self.bind_table_with_joins_at(item, base + scope.width())?;
            let left_cols = scope.width();
            let right_cols = rscope.width();
            merge_scopes(&mut scope, rscope)?;
            from = From::Join {
                left: Box::new(from),
                right: Box::new(rhs),
                kind: JoinKind::Cross,
                on: None,
                lateral: false,
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

/// A bound expression with its type.
#[derive(Clone, Debug)]
pub struct TE {
    pub e: Expr,
    pub ty: Type,
    pub typmod: i32,
    pub table_oid: u32,
    pub attnum: i16,
}

impl TE {
    pub fn new(e: Expr, ty: Type) -> TE {
        TE { e, ty, typmod: -1, table_oid: 0, attnum: 0 }
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
        })
        .collect()
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
        });
    }
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
        // Only string types accept anything else implicitly.
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
fn guess_unknown(other: Type, _op: &str) -> Type {
    if other.is_unknown() { Type::NUMERIC } else { other }
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

/// Whether a plan reads a column from `depth` levels out.
fn query_has_outer_ref(q: &Query, depth: usize) -> bool {
    let mut found = false;
    visit_query_exprs(q, &mut |e, level| {
        if let Expr::Outer(d, _) = e
            && *d == depth + level
        {
            found = true;
        }
    });
    found
}

fn visit_query_exprs(q: &Query, f: &mut dyn FnMut(&Expr, usize)) {
    fn walk_expr(e: &Expr, level: usize, f: &mut dyn FnMut(&Expr, usize)) {
        f(e, level);
        match e {
            Expr::Sub { query, .. } => visit_at(query, level + 1, f),
            Expr::InSub { query, left, .. } => {
                for l in left {
                    walk_expr(l, level, f);
                }
                visit_at(query, level + 1, f);
            }
            _ => {
                let mut cl = e.clone();
                cl.children_mut(&mut |c| walk_expr(c, level, f));
            }
        }
    }
    fn visit_at(q: &Query, level: usize, f: &mut dyn FnMut(&Expr, usize)) {
        match q {
            Query::Select(s) => {
                for e in s.proj.iter().chain(s.filter.iter()).chain(s.having.iter()) {
                    walk_expr(e, level, f);
                }
                if let Some(g) = &s.group {
                    for e in g {
                        walk_expr(e, level, f);
                    }
                }
                for agg in &s.aggs {
                    for e in &agg.args {
                        walk_expr(e, level, f);
                    }
                }
            }
            Query::Values { rows, .. } => {
                for r in rows {
                    for e in r {
                        walk_expr(e, level, f);
                    }
                }
            }
            Query::SetOp { left, right, .. } => {
                visit_at(left, level, f);
                visit_at(right, level, f);
            }
            Query::With { ctes, body } => {
                for c in ctes {
                    visit_at(&c.query, level, f);
                }
                visit_at(body, level, f);
            }
            Query::Recursive { seed, step, .. } => {
                visit_at(seed, level, f);
                visit_at(step, level, f);
            }
            Query::Dml(_) => {}
        }
    }
    visit_at(q, 0, f);
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

/// C-style escapes in E'...' strings.
fn unescape_c(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some(o) => out.push(o),
            None => {}
        }
    }
    out
}
