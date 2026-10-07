//! `EXPLAIN` / `DESCRIBE <statement>`: MySQL's traditional table, plus
//! `FORMAT=JSON`, `FORMAT=TREE` and `EXPLAIN ANALYZE`.
//!
//! noida-db has no optimizer, so the plan is read off the bound statement:
//! each table's access is the best key an equality in its conditions can
//! use (`const` / `eq_ref` for a whole unique key, `ref` for an index
//! prefix, else a full scan), with row counts taken from the data itself.
//! MySQL's cleverer choices (covering indexes, semi-join strategies, join
//! reordering) aren't imitated; the shapes and fields are.

use std::sync::Arc;

use crate::mysql::catalog::{ColumnType, Table};
use crate::mysql::plan::{CmpOp, Expr, JoinOp, Plan, SetOpKind, SortKey};
use crate::mysql::types::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Format {
    Traditional,
    Json,
    Tree,
}

impl Format {
    pub fn parse(s: &str) -> Option<Format> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "traditional" => Format::Traditional,
            "json" => Format::Json,
            "tree" => Format::Tree,
            _ => return None,
        })
    }
}

/// The traditional format's columns.
pub const COLUMNS: [&str; 12] = [
    "id",
    "select_type",
    "table",
    "partitions",
    "type",
    "possible_keys",
    "key",
    "key_len",
    "ref",
    "rows",
    "filtered",
    "Extra",
];

/// Finds a table's definition and rows.
pub type Lookup<'a> = &'a dyn Fn(&str, &str) -> Option<Arc<Table>>;

/// One table's access in a query block.
#[derive(Clone, Debug)]
struct Access {
    db: String,
    alias: String,
    table: Arc<Table>,
    /// Where the table's columns start in the joined row.
    offset: usize,
    /// Conditions evaluable once this table is read.
    conds: Vec<Expr>,
    left_join: bool,
    typ: &'static str,
    possible: Vec<String>,
    key: Option<String>,
    key_parts: Vec<String>,
    key_len: Option<usize>,
    refs: Vec<String>,
    /// Ref values, for the TREE format's `(k=1)`.
    lookup: Vec<(String, String)>,
    rows: u64,
    /// Conditions the key didn't satisfy.
    residual: Vec<Expr>,
    /// A unique lookup that found nothing.
    no_match: bool,
}

#[derive(Debug, Default)]
struct Block {
    tables: Vec<Access>,
    order: bool,
    order_keys: Vec<String>,
    group: Option<Vec<String>>,
    aggregate: Option<Vec<String>>,
    distinct: bool,
    limit: Option<(String, Option<String>)>,
    /// The SELECT list (sort keys point into it).
    project: Vec<Expr>,
}

enum Query {
    Block(Block),
    Union { all: bool, parts: Vec<Query> },
    Dml { kind: &'static str, access: Option<Access> },
    NoTables,
}

/// A table the statement names that doesn't exist (EXPLAIN fails on it).
fn missing(plan: &Plan, lookup: Lookup) -> Option<(String, String)> {
    let gone = |db: &String, t: &String| lookup(db, t).is_none().then(|| (db.clone(), t.clone()));
    match plan {
        Plan::Scan { db, table, .. }
        | Plan::Update { db, table, .. }
        | Plan::Delete { db, table, .. }
        | Plan::Insert { db, table, .. } => gone(db, table),
        Plan::Join { left, right, .. } | Plan::SetOp { left, right, .. } => {
            missing(left, lookup).or_else(|| missing(right, lookup))
        }
        Plan::InsertSelect { insert, query } => {
            missing(insert, lookup).or_else(|| missing(query, lookup))
        }
        Plan::Finish { source, .. }
        | Plan::Project { source, .. }
        | Plan::Locking { source, .. }
        | Plan::Aggregate { source, .. }
        | Plan::Filter { source, .. } => missing(source, lookup),
        Plan::Derived { plan, .. } => missing(plan, lookup),
        _ => None,
    }
}

/// Builds the explanation of `plan`; `actual` is ANALYZE's result row count.
pub fn explain(
    plan: &Plan,
    lookup: Lookup,
    format: Format,
    actual: Option<u64>,
) -> Result<Vec<Vec<Value>>, crate::mysql::error::MySqlError> {
    if let Some((db, t)) = missing(plan, lookup) {
        return Err(crate::mysql::error::MySqlError::new(
            1146,
            "42S02",
            format!("Table '{db}.{t}' doesn't exist"),
        ));
    }
    let q = query(plan, lookup);
    Ok(render_query(&q, format, actual))
}

fn render_query(q: &Query, format: Format, actual: Option<u64>) -> Vec<Vec<Value>> {
    match format {
        Format::Traditional => {
            let mut rows = vec![];
            let mut id = 1;
            traditional(q, &mut id, true, &mut rows);
            rows
        }
        Format::Json => {
            let mut id = 1;
            let doc = J::Obj(vec![("query_block".into(), json_query(q, &mut id))]);
            vec![vec![Value::Text(doc.pretty(0))]]
        }
        Format::Tree => {
            let mut lines = vec![];
            ANALYZING.with(|a| a.set(actual.is_some()));
            tree(q, 0, actual, &mut lines);
            ANALYZING.with(|a| a.set(false));
            let mut s = lines.join("\n");
            s.push('\n');
            vec![vec![Value::Text(s)]]
        }
    }
}

// ---------------------------------------------------------------------------
// Reading the plan

fn query(plan: &Plan, lookup: Lookup) -> Query {
    match plan {
        Plan::SetOp { op: SetOpKind::Union, all, left, right } => {
            let mut parts = vec![];
            for side in [left, right] {
                match query(side, lookup) {
                    Query::Union { all: a, parts: p } if a == *all => parts.extend(p),
                    other => parts.push(other),
                }
            }
            Query::Union { all: *all, parts }
        }
        Plan::Update { db, table, selection, .. } | Plan::Delete { db, table, selection, .. } => {
            let kind = if matches!(plan, Plan::Update { .. }) { "UPDATE" } else { "DELETE" };
            let access = lookup(db, table).map(|t| {
                let mut a = new_access(db, table, t, 0);
                if let Some(s) = selection {
                    let own = [a.clone()];
                    a.conds = conjuncts(s).into_iter().map(|c| resolve(c, &own)).collect();
                }
                choose(&mut a, &[], true);
                a
            });
            Query::Dml { kind, access }
        }
        Plan::MultiUpdate { .. } => Query::Dml { kind: "UPDATE", access: None },
        Plan::MultiDelete { .. } => Query::Dml { kind: "DELETE", access: None },
        Plan::Insert { db, table, .. } => {
            let access = lookup(db, table).map(|t| new_access(db, table, t, 0));
            Query::Dml { kind: "INSERT", access }
        }
        Plan::InsertSelect { insert, .. } => query(insert, lookup),
        _ => {
            let mut b = Block::default();
            block(plan, lookup, &mut b);
            if b.tables.is_empty() {
                return Query::NoTables;
            }
            let mut done: Vec<Access> = vec![];
            for mut a in std::mem::take(&mut b.tables) {
                choose(&mut a, &done, false);
                done.push(a);
            }
            b.tables = done;
            Query::Block(b)
        }
    }
}

fn new_access(db: &str, alias: &str, table: Arc<Table>, offset: usize) -> Access {
    Access {
        db: db.to_string(),
        alias: alias.to_string(),
        table,
        offset,
        conds: vec![],
        left_join: false,
        typ: "ALL",
        possible: vec![],
        key: None,
        key_parts: vec![],
        key_len: None,
        refs: vec![],
        lookup: vec![],
        rows: 0,
        residual: vec![],
        no_match: false,
    }
}

fn conjuncts(e: &Expr) -> Vec<Expr> {
    match e {
        Expr::And(v) => v.iter().flat_map(conjuncts).collect(),
        other => vec![other.clone()],
    }
}

fn width(b: &Block) -> usize {
    b.tables.iter().map(|t| t.table.columns.len()).sum()
}

fn block(plan: &Plan, lookup: Lookup, b: &mut Block) {
    match plan {
        Plan::Finish { source, order, hidden, distinct, limit, offset, .. } => {
            block(source, lookup, b);
            if !order.is_empty() {
                b.order = true;
                let visible = b.project.len().saturating_sub(*hidden);
                b.order_keys = order
                    .iter()
                    .map(|(k, asc)| {
                        let at = match k {
                            SortKey::Output(i) => *i,
                            SortKey::Hidden(i) => visible + i,
                        };
                        let k = match b.project.get(at) {
                            Some(e) => expr_text(e, b),
                            None => format!("`{}`", at + 1),
                        };
                        if *asc { k } else { format!("{k} DESC") }
                    })
                    .collect();
            }
            b.distinct |= *distinct;
            if let Some(l) = limit {
                b.limit = Some((expr_text(l, b), offset.as_ref().map(|o| expr_text(o, b))));
            }
        }
        Plan::Project { source, exprs, .. } => {
            block(source, lookup, b);
            b.project = exprs.iter().map(|e| resolve(e.clone(), &b.tables)).collect();
        }
        Plan::Locking { source, .. } => block(source, lookup, b),
        Plan::Aggregate { source, group_exprs, exprs, .. } => {
            block(source, lookup, b);
            if group_exprs.is_empty() {
                let aggs: Vec<String> = exprs.iter().filter_map(|e| agg_text(e, b)).collect();
                b.aggregate = Some(aggs);
            } else {
                b.group = Some(group_exprs.iter().map(|e| expr_text(e, b)).collect());
            }
        }
        Plan::Filter { source, predicate } => {
            block(source, lookup, b);
            for c in conjuncts(predicate) {
                attach(b, c);
            }
        }
        Plan::Join { left, right, op } => {
            block(left, lookup, b);
            let before = b.tables.len();
            block(right, lookup, b);
            let cond = match op {
                JoinOp::Inner(e) | JoinOp::Left(e) => Some(e),
                JoinOp::Cross => None,
            };
            if matches!(op, JoinOp::Left(_)) {
                for t in &mut b.tables[before..] {
                    t.left_join = true;
                }
            }
            if let Some(c) = cond {
                for c in conjuncts(c) {
                    attach(b, c);
                }
            }
        }
        Plan::Scan { db, table, alias } => {
            if let Some(t) = lookup(db, table) {
                let offset = width(b);
                let alias = alias.clone().unwrap_or_else(|| table.clone());
                b.tables.push(new_access(db, &alias, t, offset));
            }
        }
        // A derived table is merged into the outer block, as MySQL does.
        Plan::Derived { plan, .. } => block(plan, lookup, b),
        _ => {}
    }
}

/// Column names (`name`, `t.name`) as joined-row indexes.
fn resolve(e: Expr, tables: &[Access]) -> Expr {
    let r = |x: Box<Expr>| Box::new(resolve(*x, tables));
    match e {
        Expr::ColName(n) => {
            let (q, c) = match n.rsplit_once('.') {
                Some((q, c)) => {
                    (Some(q.rsplit('.').next().unwrap_or(q).to_string()), c.to_string())
                }
                None => (None, n.clone()),
            };
            let c = c.trim_matches('`');
            tables
                .iter()
                .filter(|t| {
                    q.as_deref().is_none_or(|q| t.alias.eq_ignore_ascii_case(q.trim_matches('`')))
                })
                .find_map(|t| {
                    t.table
                        .columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(c))
                        .map(|i| t.offset + i)
                })
                .map_or(Expr::ColName(n), Expr::Col)
        }
        Expr::And(v) => Expr::And(v.into_iter().map(|x| resolve(x, tables)).collect()),
        Expr::Or(v) => Expr::Or(v.into_iter().map(|x| resolve(x, tables)).collect()),
        Expr::Compare { op, left, right } => Expr::Compare { op, left: r(left), right: r(right) },
        Expr::Arith { op, left, right } => Expr::Arith { op, left: r(left), right: r(right) },
        Expr::Not(x) => Expr::Not(r(x)),
        Expr::IsNull(x, n) => Expr::IsNull(r(x), n),
        Expr::InList { expr, list, negated } => Expr::InList {
            expr: r(expr),
            list: list.into_iter().map(|x| resolve(x, tables)).collect(),
            negated,
        },
        Expr::Like { expr, pattern, escape, negated } => {
            Expr::Like { expr: r(expr), pattern: r(pattern), escape, negated }
        }
        Expr::Call { name, args } => {
            Expr::Call { name, args: args.into_iter().map(|x| resolve(x, tables)).collect() }
        }
        Expr::Agg { func, arg, distinct } => Expr::Agg { func, arg: arg.map(r), distinct },
        other => other,
    }
}

/// Puts a condition on the earliest table that has every column it reads.
fn attach(b: &mut Block, c: Expr) {
    let c = resolve(c, &b.tables);
    let mut cols = vec![];
    cols_of(&c, &mut cols);
    let max = cols.iter().copied().max();
    let idx = match max {
        None => 0,
        Some(m) => b
            .tables
            .iter()
            .position(|t| m < t.offset + t.table.columns.len())
            .unwrap_or(b.tables.len().saturating_sub(1)),
    };
    if let Some(t) = b.tables.get_mut(idx) {
        t.conds.push(c);
    }
}

fn cols_of(e: &Expr, out: &mut Vec<usize>) {
    match e {
        Expr::Col(i) => out.push(*i),
        Expr::And(v) | Expr::Or(v) => v.iter().for_each(|x| cols_of(x, out)),
        Expr::Compare { left, right, .. } | Expr::Arith { left, right, .. } => {
            cols_of(left, out);
            cols_of(right, out);
        }
        Expr::Call { args, .. } => args.iter().for_each(|x| cols_of(x, out)),
        Expr::Not(x) | Expr::IsNull(x, _) => cols_of(x, out),
        Expr::InList { expr, list, .. } => {
            cols_of(expr, out);
            list.iter().for_each(|x| cols_of(x, out));
        }
        Expr::Like { expr, pattern, .. } => {
            cols_of(expr, out);
            cols_of(pattern, out);
        }
        Expr::Agg { arg: Some(a), .. } => cols_of(a, out),
        _ => {}
    }
}

/// The table's keys: (name, columns, unique).
fn keys(t: &Table) -> Vec<(String, Vec<usize>, bool)> {
    let pos = |names: &[String]| -> Option<Vec<usize>> {
        names
            .iter()
            .map(|n| t.columns.iter().position(|c| c.name.eq_ignore_ascii_case(n)))
            .collect()
    };
    let mut out = vec![];
    let pk: Vec<usize> =
        t.columns.iter().enumerate().filter(|(_, c)| c.primary_key).map(|(i, _)| i).collect();
    if !pk.is_empty() {
        out.push(("PRIMARY".to_string(), pk, true));
    }
    for k in &t.unique_keys {
        if let Some(c) = pos(&k.columns) {
            out.push((k.name.clone(), c, true));
        }
    }
    for k in &t.indexes {
        if let Some(c) = pos(&k.columns) {
            out.push((k.name.clone(), c, false));
        }
    }
    out
}

/// `key_len`: the bytes MySQL's index entry takes (utf8mb4 strings, +1
/// for a nullable column, +2 for a VARCHAR's length).
fn key_len(t: &Table, col: usize) -> usize {
    let c = &t.columns[col];
    let base = match &c.ty {
        ColumnType::TinyInt | ColumnType::Boolean => 1,
        ColumnType::SmallInt => 2,
        ColumnType::MediumInt => 3,
        ColumnType::Int => 4,
        ColumnType::BigInt | ColumnType::Double => 8,
        ColumnType::Float => 4,
        ColumnType::Decimal(p, s) => {
            let digits =
                |n: u8| (n as usize / 9) * 4 + [0, 1, 1, 2, 2, 3, 3, 4, 4, 4][n as usize % 9];
            digits(p - s) + digits(*s)
        }
        ColumnType::Date => 3,
        ColumnType::Datetime => 5,
        ColumnType::Time(_) => 3,
        ColumnType::Varchar(n) => n * 4 + 2,
        ColumnType::Enum(_) => 1,
        ColumnType::Text | ColumnType::Blob | ColumnType::Json => 3072,
    };
    base + usize::from(!c.not_null)
}

/// What an equality on column `col` compares it with: a constant, or a
/// column of a table read earlier.
fn eq_target(e: &Expr, a: &Access, earlier: &[Access]) -> Option<(usize, String, Option<Value>)> {
    let Expr::Compare { op: CmpOp::Eq, left, right } = e else { return None };
    let own = |x: &Expr| match x {
        Expr::Col(i) if *i >= a.offset && *i < a.offset + a.table.columns.len() => {
            Some(*i - a.offset)
        }
        _ => None,
    };
    let other = |x: &Expr| -> Option<(String, Option<Value>)> {
        match x {
            Expr::Const(v) => Some(("const".into(), Some(v.clone()))),
            Expr::Param(_) => Some(("const".into(), None)),
            Expr::Col(i) => earlier.iter().find_map(|t| {
                (*i >= t.offset && *i < t.offset + t.table.columns.len()).then(|| {
                    let c = &t.table.columns[*i - t.offset].name;
                    (format!("{}.{}.{}", t.db, t.alias, c), None)
                })
            }),
            _ => None,
        }
    };
    match (own(left), own(right)) {
        (Some(c), None) => other(right).map(|(r, v)| (c, r, v)),
        (None, Some(c)) => other(left).map(|(r, v)| (c, r, v)),
        _ => None,
    }
}

fn is_range_on(e: &Expr, a: &Access) -> Option<usize> {
    let Expr::Compare { op, left, right } = e else { return None };
    if *op == CmpOp::Ne {
        return None;
    }
    [left, right].into_iter().find_map(|x| match x.as_ref() {
        Expr::Col(i) if *i >= a.offset && *i < a.offset + a.table.columns.len() => {
            Some(*i - a.offset)
        }
        _ => None,
    })
}

/// Picks the table's access path.
fn choose(a: &mut Access, earlier: &[Access], dml: bool) {
    let total = a.table.rows.len() as u64;
    let eqs: Vec<(usize, String, Option<Value>, usize)> = a
        .conds
        .iter()
        .enumerate()
        .filter_map(|(i, c)| eq_target(c, a, earlier).map(|(col, r, v)| (col, r, v, i)))
        .collect();
    let ranged: Vec<usize> = a.conds.iter().filter_map(|c| is_range_on(c, a)).collect();
    let ks = keys(&a.table);
    a.possible = ks
        .iter()
        .filter(|(_, cols, _)| ranged.contains(&cols[0]))
        .map(|(n, _, _)| n.clone())
        .collect();
    a.rows = total;
    // Best usable key: a whole unique key, else the longest index prefix.
    let mut best: Option<(&String, Vec<usize>, bool)> = None;
    for (name, cols, unique) in &ks {
        let prefix: Vec<usize> =
            cols.iter().take_while(|c| eqs.iter().any(|e| e.0 == **c)).copied().collect();
        if prefix.is_empty() {
            continue;
        }
        let whole = *unique && prefix.len() == cols.len();
        let better = match &best {
            None => true,
            Some((_, p, w)) => (whole && !w) || (whole == *w && prefix.len() > p.len()),
        };
        if better {
            best = Some((name, prefix, whole));
        }
    }
    let mut used = vec![];
    if let Some((name, prefix, whole)) = best {
        a.key = Some(name.clone());
        a.key_parts = prefix.iter().map(|&c| a.table.columns[c].name.clone()).collect();
        a.key_len = Some(prefix.iter().map(|&c| key_len(&a.table, c)).sum());
        let mut consts = vec![];
        for &c in &prefix {
            let (_, r, v, i) = eqs.iter().find(|e| e.0 == c).unwrap();
            a.refs.push(r.clone());
            let shown = match v {
                Some(v) => value_text(v),
                None if r == "const" => "?".to_string(),
                None => r.split('.').skip(1).collect::<Vec<_>>().join("."),
            };
            a.lookup.push((a.table.columns[c].name.clone(), shown));
            used.push(*i);
            consts.push((c, v.clone()));
        }
        let all_const = consts.iter().all(|(_, v)| v.is_some());
        let matching = if all_const {
            a.table
                .rows
                .iter()
                .filter(|row| {
                    consts.iter().all(|(c, v)| {
                        crate::mysql::exec::mysql_cmp(&row[*c], v.as_ref().unwrap())
                            == Some(std::cmp::Ordering::Equal)
                    })
                })
                .count() as u64
        } else {
            1
        };
        let all_refs_const = a.refs.iter().all(|r| r == "const");
        a.typ = match (whole, all_refs_const, dml) {
            (_, true, true) => "range",
            (true, true, false) => "const",
            (true, false, _) => "eq_ref",
            (false, _, _) => "ref",
        };
        a.no_match = a.typ == "const" && all_const && matching == 0;
        a.rows = if whole { 1 } else { matching.max(1) };
        if !a.possible.contains(name) {
            a.possible.push(name.clone());
        }
    }
    a.residual = a
        .conds
        .iter()
        .enumerate()
        .filter(|(i, _)| !used.contains(i))
        .map(|(_, c)| c.clone())
        .collect();
}

fn filtered(a: &Access) -> f64 {
    if a.residual.is_empty() || a.rows == 0 { 100.0 } else { (100.0 / a.rows as f64).min(100.0) }
}

// ---------------------------------------------------------------------------
// Expressions as MySQL prints them

fn value_text(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "\\'")),
        Value::Bytes(_) | Value::Date(_) | Value::Ts(_) | Value::Time(_) | Value::Json(_) => {
            format!("'{}'", crate::mysql::exec::render_text(v))
        }
        other => crate::mysql::exec::render_text(other),
    }
}

/// The column a joined-row index names, as `alias.`col``.
fn col_text(i: usize, tables: &[Access]) -> String {
    tables
        .iter()
        .find(|t| i >= t.offset && i < t.offset + t.table.columns.len())
        .map(|t| format!("{}.`{}`", t.alias, t.table.columns[i - t.offset].name))
        .unwrap_or_else(|| format!("<column {i}>"))
}

fn expr_text(e: &Expr, b: &Block) -> String {
    expr_in(e, &b.tables)
}

fn expr_in(e: &Expr, tables: &[Access]) -> String {
    let op = |o: &CmpOp| match o {
        CmpOp::Eq => "=",
        CmpOp::Ne => "<>",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    };
    match e {
        Expr::Const(v) => value_text(v),
        Expr::Param(_) => "?".into(),
        Expr::Col(i) => col_text(*i, tables),
        Expr::ColName(n) => format!("`{n}`"),
        Expr::And(v) => {
            format!("({})", v.iter().map(|x| expr_in(x, tables)).collect::<Vec<_>>().join(" and "))
        }
        Expr::Or(v) => {
            format!("({})", v.iter().map(|x| expr_in(x, tables)).collect::<Vec<_>>().join(" or "))
        }
        Expr::Compare { op: o, left, right } => {
            format!("({} {} {})", expr_in(left, tables), op(o), expr_in(right, tables))
        }
        Expr::Not(x) => format!("(not({}))", expr_in(x, tables)),
        Expr::IsNull(x, negated) => {
            format!("({} is {}null)", expr_in(x, tables), if *negated { "not " } else { "" })
        }
        Expr::InList { expr, list, negated } => format!(
            "({} {}in ({}))",
            expr_in(expr, tables),
            if *negated { "not " } else { "" },
            list.iter().map(|x| expr_in(x, tables)).collect::<Vec<_>>().join(",")
        ),
        Expr::Like { expr, pattern, negated, .. } => format!(
            "({} {}like {})",
            expr_in(expr, tables),
            if *negated { "not " } else { "" },
            expr_in(pattern, tables)
        ),
        Expr::Call { name, args } => format!(
            "{}({})",
            name.to_ascii_lowercase(),
            args.iter().map(|x| expr_in(x, tables)).collect::<Vec<_>>().join(",")
        ),
        Expr::Agg { func, arg, distinct } => {
            let a = match arg {
                Some(a) => expr_in(a, tables),
                None => "0".into(),
            };
            format!(
                "{}({}{a})",
                format!("{func:?}").to_ascii_lowercase(),
                if *distinct { "distinct " } else { "" }
            )
        }
        _ => "<expr>".into(),
    }
}

fn agg_text(e: &Expr, b: &Block) -> Option<String> {
    match e {
        Expr::Agg { .. } => Some(expr_text(e, b)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Traditional

fn traditional(q: &Query, id: &mut u64, top: bool, out: &mut Vec<Vec<Value>>) {
    let t = |s: &str| Value::Text(s.to_string());
    match q {
        Query::NoTables => out.push(row(*id, "SIMPLE", None, None, Some("No tables used"))),
        Query::Dml { kind, access } => {
            let extra = access.as_ref().and_then(|a| {
                (!a.residual.is_empty() || *kind != "INSERT" && a.key.is_some())
                    .then_some("Using where")
            });
            match access {
                Some(a) if *kind != "INSERT" => {
                    out.push(access_row(*id, kind, a, extra.map(String::from)))
                }
                Some(a) => out.push(vec![
                    Value::Int(*id as i64),
                    t(kind),
                    t(&a.alias),
                    Value::Null,
                    t("ALL"),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]),
                None => out.push(row(*id, kind, None, None, None)),
            }
        }
        Query::Block(b) => {
            let select_type = if top { "SIMPLE" } else { "UNION" };
            let select_type = if top && *id > 1 { "PRIMARY" } else { select_type };
            if b.tables.len() == 1 && b.tables[0].no_match {
                out.push(row(*id, select_type, None, None, Some("no matching row in const table")));
                return;
            }
            for (i, a) in b.tables.iter().enumerate() {
                let mut extra = vec![];
                if !a.residual.is_empty() {
                    extra.push("Using where".to_string());
                }
                if i == 0 {
                    if b.group.is_some() || b.distinct {
                        extra.push("Using temporary".into());
                    }
                    if b.order {
                        extra.push("Using filesort".into());
                    }
                }
                if i > 0 && a.typ == "ALL" {
                    extra.push("Using join buffer (hash join)".into());
                }
                let extra = (!extra.is_empty()).then(|| extra.join("; "));
                out.push(access_row(*id, select_type, a, extra));
            }
        }
        Query::Union { all, parts } => {
            let first = *id;
            for (n, p) in parts.iter().enumerate() {
                let start = out.len();
                traditional(p, id, n == 0, out);
                for r in &mut out[start..] {
                    if n == 0 {
                        if r[1] == t("SIMPLE") {
                            r[1] = t("PRIMARY");
                        }
                    } else {
                        r[1] = t("UNION");
                    }
                }
                *id += 1;
            }
            if !all {
                let ids: Vec<String> = (first..*id).map(|i| i.to_string()).collect();
                out.push(vec![
                    Value::Int(*id as i64),
                    t("UNION RESULT"),
                    t(&format!("<union{}>", ids.join(","))),
                    Value::Null,
                    t("ALL"),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    t("Using temporary"),
                ]);
            }
        }
    }
}

fn row(
    id: u64,
    select_type: &str,
    table: Option<&str>,
    typ: Option<&str>,
    extra: Option<&str>,
) -> Vec<Value> {
    let o = |s: Option<&str>| s.map(|s| Value::Text(s.to_string())).unwrap_or(Value::Null);
    vec![
        Value::Int(id as i64),
        Value::Text(select_type.to_string()),
        o(table),
        Value::Null,
        o(typ),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        o(extra),
    ]
}

fn access_row(id: u64, select_type: &str, a: &Access, extra: Option<String>) -> Vec<Value> {
    let o = |s: Option<String>| s.map(Value::Text).unwrap_or(Value::Null);
    vec![
        Value::Int(id as i64),
        Value::Text(select_type.to_string()),
        Value::Text(a.alias.clone()),
        Value::Null,
        Value::Text(a.typ.to_string()),
        o((!a.possible.is_empty()).then(|| a.possible.join(","))),
        o(a.key.clone()),
        o(a.key_len.map(|l| l.to_string())),
        o((!a.refs.is_empty()).then(|| a.refs.join(","))),
        Value::Int(a.rows as i64),
        // A FLOAT column in MySQL (two decimals).
        Value::Float((filtered(a) * 100.0).round() / 100.0),
        o(extra),
    ]
}

// ---------------------------------------------------------------------------
// JSON (fields in MySQL's order)

enum J {
    Obj(Vec<(String, J)>),
    Arr(Vec<J>),
    Str(String),
    Num(String),
    Bool(bool),
}

impl J {
    fn s(v: impl Into<String>) -> J {
        J::Str(v.into())
    }

    fn pretty(&self, level: usize) -> String {
        let ind = "  ".repeat(level + 1);
        let end = "  ".repeat(level);
        match self {
            J::Obj(fields) => {
                let body: Vec<String> = fields
                    .iter()
                    .map(|(k, v)| format!("{ind}{}: {}", quote(k), v.pretty(level + 1)))
                    .collect();
                format!("{{\n{}\n{end}}}", body.join(",\n"))
            }
            J::Arr(items) => {
                let body: Vec<String> =
                    items.iter().map(|v| format!("{ind}{}", v.pretty(level + 1))).collect();
                format!("[\n{}\n{end}]", body.join(",\n"))
            }
            J::Str(s) => quote(s),
            J::Num(n) => n.clone(),
            J::Bool(b) => b.to_string(),
        }
    }
}

fn quote(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

fn cost(x: f64) -> String {
    format!("{x:.2}")
}

fn read_cost(a: &Access) -> f64 {
    0.25 + a.rows as f64 * 0.1
}

fn json_table(a: &Access, b: Option<&Block>) -> J {
    let mut f: Vec<(String, J)> =
        vec![("table_name".into(), J::s(&a.alias)), ("access_type".into(), J::s(a.typ))];
    if !a.possible.is_empty() {
        f.push(("possible_keys".into(), J::Arr(a.possible.iter().map(J::s).collect())));
    }
    if let Some(k) = &a.key {
        f.push(("key".into(), J::s(k)));
        f.push(("used_key_parts".into(), J::Arr(a.key_parts.iter().map(J::s).collect())));
        f.push(("key_length".into(), J::s(a.key_len.unwrap_or(0).to_string())));
        f.push(("ref".into(), J::Arr(a.refs.iter().map(J::s).collect())));
    }
    let produced = ((a.rows as f64) * filtered(a) / 100.0).ceil() as u64;
    f.push(("rows_examined_per_scan".into(), J::Num(a.rows.to_string())));
    f.push(("rows_produced_per_join".into(), J::Num(produced.to_string())));
    f.push(("filtered".into(), J::s(format!("{:.2}", filtered(a)))));
    let eval = produced as f64 * 0.1;
    f.push((
        "cost_info".into(),
        J::Obj(vec![
            ("read_cost".into(), J::s(cost(read_cost(a)))),
            ("eval_cost".into(), J::s(cost(eval))),
            ("prefix_cost".into(), J::s(cost(read_cost(a) + eval))),
            ("data_read_per_join".into(), J::s((produced * 152).to_string())),
        ]),
    ));
    f.push((
        "used_columns".into(),
        J::Arr(a.table.columns.iter().map(|c| J::s(&c.name)).collect()),
    ));
    if !a.residual.is_empty() {
        let tables = b.map_or(std::slice::from_ref(a), |b| &b.tables[..]);
        let cond = if a.residual.len() == 1 {
            expr_in(&a.residual[0], tables)
        } else {
            expr_in(&Expr::And(a.residual.clone()), tables)
        };
        f.push(("attached_condition".into(), J::s(qualify(&cond, a))));
    }
    J::Obj(f)
}

/// JSON's attached_condition names columns `db`.`table`.`col`.
fn qualify(cond: &str, a: &Access) -> String {
    cond.replace(&format!("{}.`", a.alias), &format!("`{}`.`{}`.`", a.db, a.alias))
}

fn json_query(q: &Query, id: &mut u64) -> J {
    match q {
        Query::NoTables => J::Obj(vec![
            ("select_id".into(), J::Num(id.to_string())),
            ("message".into(), J::s("No tables used")),
        ]),
        Query::Dml { kind, access } => {
            let mut f = vec![("select_id".into(), J::Num(id.to_string()))];
            if let Some(a) = access {
                let mut t = json_table(a, None);
                if let J::Obj(fields) = &mut t {
                    fields.insert(0, (kind.to_ascii_lowercase(), J::Bool(true)));
                }
                f.push(("table".into(), t));
            }
            J::Obj(f)
        }
        Query::Block(b) => {
            let total: f64 = b.tables.iter().map(|a| read_cost(a) + a.rows as f64 * 0.1).sum();
            let mut inner: Vec<(String, J)> = if b.tables.len() == 1 {
                vec![("table".into(), json_table(&b.tables[0], Some(b)))]
            } else {
                vec![(
                    "nested_loop".into(),
                    J::Arr(
                        b.tables
                            .iter()
                            .map(|a| J::Obj(vec![("table".into(), json_table(a, Some(b)))]))
                            .collect(),
                    ),
                )]
            };
            if b.group.is_some() {
                let mut g = vec![
                    ("using_temporary_table".into(), J::Bool(true)),
                    ("using_filesort".into(), J::Bool(false)),
                ];
                g.append(&mut inner);
                inner = vec![("grouping_operation".into(), J::Obj(g))];
            }
            if b.distinct {
                let mut d = vec![("using_temporary_table".into(), J::Bool(true))];
                d.append(&mut inner);
                inner = vec![("duplicates_removal".into(), J::Obj(d))];
            }
            if b.order {
                let mut o = vec![("using_filesort".into(), J::Bool(true))];
                o.append(&mut inner);
                inner = vec![("ordering_operation".into(), J::Obj(o))];
            }
            let mut f = vec![
                ("select_id".into(), J::Num(id.to_string())),
                ("cost_info".into(), J::Obj(vec![("query_cost".into(), J::s(cost(total)))])),
            ];
            f.append(&mut inner);
            J::Obj(f)
        }
        Query::Union { all, parts } => {
            let first = *id;
            let mut specs = vec![];
            for p in parts {
                specs.push(J::Obj(vec![
                    ("dependent".into(), J::Bool(false)),
                    ("cacheable".into(), J::Bool(true)),
                    ("query_block".into(), json_query(p, id)),
                ]));
                *id += 1;
            }
            let mut u = vec![("using_temporary_table".into(), J::Bool(!all))];
            if !all {
                let ids: Vec<String> = (first..*id).map(|i| i.to_string()).collect();
                u.push(("table_name".into(), J::s(format!("<union{}>", ids.join(",")))));
                u.push(("access_type".into(), J::s("ALL")));
            }
            u.push(("query_specifications".into(), J::Arr(specs)));
            J::Obj(vec![("union_result".into(), J::Obj(u))])
        }
    }
}

// ---------------------------------------------------------------------------
// TREE / ANALYZE

fn num(x: f64) -> String {
    let s = format!("{x:.2}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() { "0".into() } else { s.to_string() }
}

thread_local! {
    /// Set while rendering EXPLAIN ANALYZE: every node gets actual figures
    /// (the measured count at the top, the estimate below).
    static ANALYZING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn tree_line(depth: usize, text: &str, cost_v: f64, rows: f64, actual: Option<u64>) -> String {
    let mut s =
        format!("{}-> {text}  (cost={} rows={})", "    ".repeat(depth), num(cost_v), num(rows));
    let shown = match actual {
        Some(n) => Some(n.to_string()),
        None if ANALYZING.with(|a| a.get()) => Some(num(rows.round())),
        None => None,
    };
    if let Some(n) = shown {
        s.push_str(&format!(" (actual time=0.01..0.02 rows={n} loops=1)"));
    }
    s
}

fn tree(q: &Query, depth: usize, actual: Option<u64>, out: &mut Vec<String>) {
    match q {
        Query::NoTables => out.push(format!(
            "{}-> Rows fetched before execution  (cost=0..0 rows=1)",
            "    ".repeat(depth)
        )),
        Query::Dml { .. } => out.push("<not executable by iterator executor>".into()),
        Query::Union { all, parts } => {
            let rows: f64 = parts.len() as f64;
            if *all {
                out.push(tree_line(depth, "Append", rows, rows, actual));
                for p in parts {
                    out.push(format!(
                        "{}-> Stream results  (cost=0.55 rows=1)",
                        "    ".repeat(depth + 1)
                    ));
                    tree(p, depth + 2, None, out);
                }
            } else {
                out.push(tree_line(depth, "Table scan on <union temporary>", rows, rows, actual));
                out.push(tree_line(
                    depth + 1,
                    "Union materialize with deduplication",
                    rows,
                    rows,
                    None,
                ));
                for p in parts {
                    tree(p, depth + 2, None, out);
                }
            }
        }
        Query::Block(b) => {
            if b.tables.len() == 1 && (b.tables[0].typ == "const") && b.group.is_none() {
                let text = if b.tables[0].no_match {
                    "Zero rows (no matching row in const table)  (cost=0..0 rows=0)"
                } else {
                    "Rows fetched before execution  (cost=0..0 rows=1)"
                };
                out.push(format!("{}-> {text}", "    ".repeat(depth)));
                return;
            }
            let rows = b
                .tables
                .iter()
                .map(|a| a.rows.max(1) as f64 * filtered(a) / 100.0)
                .product::<f64>();
            let mut d = depth;
            let mut top = actual;
            let mut wrap = |text: String, out: &mut Vec<String>, d: &mut usize| {
                out.push(tree_line(*d, &text, rows, rows, top.take()));
                *d += 1;
            };
            if let Some((l, off)) = &b.limit {
                let text = match off {
                    Some(o) => format!("Limit/Offset: {l}/{o} row(s)"),
                    None => format!("Limit: {l} row(s)"),
                };
                wrap(text, out, &mut d);
            }
            if b.order {
                let mut text = format!("Sort: {}", b.order_keys.join(", "));
                if let Some((l, None)) = &b.limit {
                    text.push_str(&format!(", limit input to {l} row(s) per chunk"));
                }
                wrap(text, out, &mut d);
            }
            if b.distinct {
                wrap("Table scan on <temporary>".into(), out, &mut d);
                wrap("Temporary table with deduplication".into(), out, &mut d);
            }
            if b.group.is_some() {
                wrap("Table scan on <temporary>".into(), out, &mut d);
                wrap("Aggregate using temporary table".into(), out, &mut d);
            } else if let Some(aggs) = &b.aggregate {
                wrap(format!("Aggregate: {}", aggs.join(", ")), out, &mut d);
            }
            tables_tree(b, b.tables.len(), d, top, out);
        }
    }
}

/// The first `n` tables as a left-deep nested loop.
fn tables_tree(b: &Block, n: usize, depth: usize, actual: Option<u64>, out: &mut Vec<String>) {
    if n == 1 {
        access_tree(b, &b.tables[0], depth, actual, out);
        return;
    }
    let right = &b.tables[n - 1];
    let kind = if right.left_join { "Nested loop left join" } else { "Nested loop inner join" };
    let rows = b.tables[..n].iter().map(|a| a.rows.max(1) as f64).product::<f64>();
    out.push(tree_line(depth, kind, rows, rows, actual));
    tables_tree(b, n - 1, depth + 1, None, out);
    access_tree(b, right, depth + 1, None, out);
}

fn access_tree(b: &Block, a: &Access, depth: usize, actual: Option<u64>, out: &mut Vec<String>) {
    let mut d = depth;
    let mut top = actual;
    let produced = (a.rows as f64 * filtered(a) / 100.0).max(if a.rows == 0 { 0.0 } else { 1.0 });
    if !a.residual.is_empty() {
        let cond = if a.residual.len() == 1 {
            expr_in(&a.residual[0], &b.tables)
        } else {
            expr_in(&Expr::And(a.residual.clone()), &b.tables)
        };
        out.push(tree_line(d, &format!("Filter: {cond}"), read_cost(a), produced, top.take()));
        d += 1;
    }
    let lookup = a.lookup.iter().map(|(c, v)| format!("{c}={v}")).collect::<Vec<_>>().join(", ");
    let text = match (a.typ, &a.key) {
        ("eq_ref" | "const", Some(k)) => {
            format!("Single-row index lookup on {} using {k} ({lookup})", a.alias)
        }
        ("ref" | "range", Some(k)) => format!("Index lookup on {} using {k} ({lookup})", a.alias),
        _ => format!("Table scan on {}", a.alias),
    };
    out.push(tree_line(d, &text, read_cost(a), a.rows as f64, top.take()));
}
