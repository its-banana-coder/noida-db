//! Executes parsed statements: `SELECT` (scalar, `system.one`/`numbers(N)`/
//! `system.tables`, and real tables, with `WHERE`/`GROUP BY`/`ORDER BY`/
//! `LIMIT`/`FINAL`), `CREATE TABLE`, `CREATE MATERIALIZED VIEW ... TO`,
//! `INSERT` (literal `VALUES` or a `FORMAT`-encoded body), `DROP TABLE` and
//! `OPTIMIZE TABLE ... FINAL`. One `Engine` per server, shared by every
//! connection behind a `Mutex` — same shape as `redis::engine`, since
//! noida-db's services all trade throughput for simplicity (see
//! docs/specs/README.md).

use std::collections::HashSet;
use std::sync::Mutex;

use super::catalog::{self, Catalog, MaterializedView};
use super::error::ChError;
use super::rowbinary;
use super::sql::{
    self, BinOp, CreateMaterializedView, CreateTable, DropTable, Expr, Insert, InsertSource,
    OptimizeTable, Select, Statement, Table,
};
use super::types::{self, Type, Val};

pub const SERVER_VERSION: &str = "24.8.4.13";

pub(crate) type Columns = Vec<(String, Type)>;
pub(crate) type Rows = Vec<Vec<Val>>;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<(String, Type)>,
    pub rows: Vec<Vec<Val>>,
}

fn empty_result() -> QueryResult {
    QueryResult { columns: vec![], rows: vec![] }
}

pub struct Engine {
    catalog: Mutex<Catalog>,
}

impl Default for Engine {
    fn default() -> Engine {
        Engine::new()
    }
}

impl Engine {
    pub fn new() -> Engine {
        Engine { catalog: Mutex::new(Catalog::new()) }
    }

    /// Parses and runs `sql`, returning the result and the `FORMAT` clause
    /// (if any) so the caller can pick an output format. `body` is the raw
    /// HTTP request body, used only by `INSERT ... FORMAT <fmt>` (the rows
    /// are `<fmt>`-encoded data there, not SQL).
    pub fn execute(
        &self,
        sql: &str,
        body: &[u8],
    ) -> Result<(QueryResult, Option<String>), ChError> {
        match sql::parse(sql)? {
            Statement::Select(s) => {
                let format = s.format.clone();
                let catalog = self.catalog.lock().unwrap();
                Ok((run_select(&catalog, &s)?, format))
            }
            Statement::CreateTable(c) => {
                let mut catalog = self.catalog.lock().unwrap();
                Ok((do_create(&mut catalog, c)?, None))
            }
            Statement::CreateMaterializedView(m) => {
                let mut catalog = self.catalog.lock().unwrap();
                Ok((do_create_mv(&mut catalog, m)?, None))
            }
            Statement::Insert(i) => {
                let mut catalog = self.catalog.lock().unwrap();
                Ok((do_insert(&mut catalog, i, body)?, None))
            }
            Statement::DropTable(d) => {
                let mut catalog = self.catalog.lock().unwrap();
                Ok((do_drop(&mut catalog, d)?, None))
            }
            Statement::OptimizeTable(o) => {
                let mut catalog = self.catalog.lock().unwrap();
                Ok((do_optimize(&mut catalog, o)?, None))
            }
        }
    }
}

// ---- DDL / DML -------------------------------------------------------

fn expr_to_ident(e: &Expr) -> Result<String, ChError> {
    match e {
        Expr::Ident(name) => Ok(name.clone()),
        other => {
            Err(ChError::not_implemented(&format!("engine argument {}", other.default_name())))
        }
    }
}

fn do_create(catalog: &mut Catalog, c: CreateTable) -> Result<QueryResult, ChError> {
    let database = c.database.clone().unwrap_or_else(|| "default".into());
    if catalog.exists(&database, &c.table) {
        if c.if_not_exists {
            return Ok(empty_result());
        }
        return Err(ChError::table_already_exists(&database, &c.table));
    }
    let mut columns = Vec::with_capacity(c.columns.len());
    for (name, type_name) in &c.columns {
        let ty = Type::parse(type_name)
            .ok_or_else(|| ChError::not_implemented(&format!("column type {type_name}")))?;
        columns.push((name.clone(), ty));
    }
    let engine = match c.engine.as_str() {
        "Memory" => catalog::Engine::Memory,
        "MergeTree" => catalog::Engine::MergeTree,
        "ReplacingMergeTree" => {
            let mut args = c.engine_args.iter();
            let ver = args.next().map(expr_to_ident).transpose()?;
            let is_deleted = args.next().map(expr_to_ident).transpose()?;
            catalog::Engine::ReplacingMergeTree { ver, is_deleted }
        }
        "SummingMergeTree" => {
            let sum_columns = if c.engine_args.is_empty() {
                None
            } else {
                Some(c.engine_args.iter().map(expr_to_ident).collect::<Result<Vec<_>, _>>()?)
            };
            catalog::Engine::SummingMergeTree { sum_columns }
        }
        other => return Err(ChError::not_implemented(&format!("ENGINE = {other}"))),
    };
    catalog.create(
        &database,
        &c.table,
        catalog::Table { columns, engine, order_by: c.order_by, rows: vec![] },
    );
    Ok(empty_result())
}

fn do_create_mv(catalog: &mut Catalog, m: CreateMaterializedView) -> Result<QueryResult, ChError> {
    let database = m.database.clone().unwrap_or_else(|| "default".into());
    if catalog.mv_exists(&database, &m.name) {
        if m.if_not_exists {
            return Ok(empty_result());
        }
        return Err(ChError::table_already_exists(&database, &m.name));
    }
    let Table::Named { database: src_db, table: src_table } = &m.select.table else {
        return Err(ChError::not_implemented(
            "CREATE MATERIALIZED VIEW over a table function or system table",
        ));
    };
    let source_db = src_db.clone().unwrap_or_else(|| "default".into());
    if !catalog.exists(&source_db, src_table) {
        return Err(ChError::unknown_table(&source_db, src_table));
    }
    let to_database = m.to_database.clone().unwrap_or_else(|| "default".into());
    if !catalog.exists(&to_database, &m.to_table) {
        // The "TO" form writes into a table that must already exist.
        return Err(ChError::unknown_table(&to_database, &m.to_table));
    }
    catalog.create_mv(
        &database,
        &m.name,
        MaterializedView {
            source: (source_db, src_table.clone()),
            target: (to_database, m.to_table.clone()),
            select: m.select,
        },
    );
    Ok(empty_result())
}

fn resolve_col_idxs(
    schema: &Columns,
    columns: &Option<Vec<String>>,
) -> Result<Vec<usize>, ChError> {
    match columns {
        Some(names) => names
            .iter()
            .map(|n| {
                schema
                    .iter()
                    .position(|(cn, _)| cn == n)
                    .ok_or_else(|| ChError::unknown_identifier(n))
            })
            .collect(),
        None => Ok((0..schema.len()).collect()),
    }
}

fn do_insert(catalog: &mut Catalog, i: Insert, body: &[u8]) -> Result<QueryResult, ChError> {
    let database = i.database.clone().unwrap_or_else(|| "default".into());
    let schema = catalog
        .get(&database, &i.table)
        .ok_or_else(|| ChError::unknown_table(&database, &i.table))?
        .columns
        .clone();
    let col_idxs = resolve_col_idxs(&schema, &i.columns)?;

    let new_rows: Rows = match &i.source {
        InsertSource::Values(value_rows) => {
            let mut rows = Vec::with_capacity(value_rows.len());
            for values in value_rows {
                if values.len() != col_idxs.len() {
                    return Err(ChError::syntax("INSERT: wrong number of values in a row"));
                }
                let mut row: Vec<Val> = schema.iter().map(|(_, t)| types::zero_value(*t)).collect();
                for (pos, expr) in values.iter().enumerate() {
                    let (_, v) = eval_row_expr(expr, &[], &[])?;
                    row[col_idxs[pos]] = types::coerce(&v, schema[col_idxs[pos]].1)?;
                }
                rows.push(row);
            }
            rows
        }
        InsertSource::Format(fmt) => {
            let target_schema: Columns = col_idxs.iter().map(|&idx| schema[idx].clone()).collect();
            let decoded = match fmt.as_str() {
                "RowBinary" => rowbinary::decode_rows(body, &target_schema, false, false)?,
                "RowBinaryWithNames" => rowbinary::decode_rows(body, &target_schema, true, false)?,
                "RowBinaryWithNamesAndTypes" => {
                    rowbinary::decode_rows(body, &target_schema, true, true)?
                }
                other => {
                    return Err(ChError::not_implemented(&format!("INSERT ... FORMAT {other}")));
                }
            };
            decoded
                .into_iter()
                .map(|values| {
                    let mut row: Vec<Val> =
                        schema.iter().map(|(_, t)| types::zero_value(*t)).collect();
                    for (pos, v) in values.into_iter().enumerate() {
                        row[col_idxs[pos]] = types::coerce(&v, schema[col_idxs[pos]].1)?;
                    }
                    Ok(row)
                })
                .collect::<Result<Vec<_>, ChError>>()?
        }
    };

    catalog.get_mut(&database, &i.table).unwrap().rows.extend(new_rows.clone());

    // Materialized views watching this table get just the newly inserted
    // block, the way ClickHouse's "TO" form works — not the whole table.
    for mv in catalog.mvs_for_source(&database, &i.table) {
        let transformed = transform_select(&mv.select, &schema, new_rows.clone())?;
        let (tdb, ttable) = &mv.target;
        let target_schema = catalog
            .get(tdb, ttable)
            .ok_or_else(|| ChError::unknown_table(tdb, ttable))?
            .columns
            .clone();
        let mut out_rows = Vec::with_capacity(transformed.rows.len());
        for row in transformed.rows {
            let mut coerced = Vec::with_capacity(target_schema.len());
            for (idx, (_, ty)) in target_schema.iter().enumerate() {
                let v = row.get(idx).cloned().unwrap_or_else(|| types::zero_value(*ty));
                coerced.push(types::coerce(&v, *ty)?);
            }
            out_rows.push(coerced);
        }
        catalog.get_mut(tdb, ttable).unwrap().rows.extend(out_rows);
    }
    Ok(empty_result())
}

fn do_drop(catalog: &mut Catalog, d: DropTable) -> Result<QueryResult, ChError> {
    let database = d.database.clone().unwrap_or_else(|| "default".into());
    if !catalog.drop(&database, &d.table) && !d.if_exists {
        return Err(ChError::unknown_table(&database, &d.table));
    }
    Ok(empty_result())
}

fn do_optimize(catalog: &mut Catalog, o: OptimizeTable) -> Result<QueryResult, ChError> {
    let database = o.database.clone().unwrap_or_else(|| "default".into());
    // No background merges exist to trigger, so a bare OPTIMIZE (no FINAL)
    // is a no-op; it still checks the table exists, the way real
    // ClickHouse would refuse an unknown one.
    let t = catalog
        .get(&database, &o.table)
        .ok_or_else(|| ChError::unknown_table(&database, &o.table))?;
    if !o.final_ {
        return Ok(empty_result());
    }
    let merged = merge_final(&t.engine, &t.order_by, &t.columns, &t.rows)?;
    catalog.get_mut(&database, &o.table).unwrap().rows = merged;
    Ok(empty_result())
}

// ---- ReplacingMergeTree / SummingMergeTree merge semantics --------------
//
// Real ClickHouse stores each INSERT as a part and merges parts in the
// background (nondeterministic timing); `SELECT ... FINAL` and `OPTIMIZE
// TABLE ... FINAL` are the deterministic ways to observe the merged state,
// so that's what's built here — applied on demand over the whole table,
// since there are no parts to merge incrementally. See docs/LIMITATIONS.md.

fn merge_final(
    engine: &catalog::Engine,
    order_by: &[String],
    columns: &Columns,
    rows: &Rows,
) -> Result<Rows, ChError> {
    match engine {
        catalog::Engine::ReplacingMergeTree { ver, is_deleted } => {
            replacing_final(order_by, ver.as_deref(), is_deleted.as_deref(), columns, rows)
        }
        catalog::Engine::SummingMergeTree { sum_columns } => {
            summing_final(order_by, sum_columns.as_deref(), columns, rows)
        }
        catalog::Engine::Memory | catalog::Engine::MergeTree => Ok(rows.clone()),
    }
}

fn column_index(columns: &Columns, name: &str) -> Result<usize, ChError> {
    columns.iter().position(|(n, _)| n == name).ok_or_else(|| ChError::unknown_identifier(name))
}

fn group_key_text(row: &[Val], key_idxs: &[usize]) -> String {
    key_idxs.iter().map(|&i| val_text(&row[i])).collect::<Vec<_>>().join("\u{1}")
}

/// Keeps, per `ORDER BY` key, the row with the greatest `ver` (or the last
/// inserted if there's no `ver` column), then drops rows where `is_deleted`
/// is true.
fn replacing_final(
    order_by: &[String],
    ver: Option<&str>,
    is_deleted: Option<&str>,
    columns: &Columns,
    rows: &Rows,
) -> Result<Rows, ChError> {
    if order_by.is_empty() {
        return Ok(rows.clone());
    }
    let key_idxs: Vec<usize> =
        order_by.iter().map(|n| column_index(columns, n)).collect::<Result<_, _>>()?;
    let ver_idx = ver.map(|n| column_index(columns, n)).transpose()?;
    let deleted_idx = is_deleted.map(|n| column_index(columns, n)).transpose()?;

    let mut pos_of: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut out: Rows = Vec::new();
    for row in rows {
        let key = group_key_text(row, &key_idxs);
        match pos_of.get(&key) {
            Some(&pos) => {
                let replace = match ver_idx {
                    Some(vi) => {
                        !matches!(compare(&row[vi], &out[pos][vi]), Some(std::cmp::Ordering::Less))
                    }
                    None => true, // no version column: last inserted wins
                };
                if replace {
                    out[pos] = row.clone();
                }
            }
            None => {
                pos_of.insert(key, out.len());
                out.push(row.clone());
            }
        }
    }
    if let Some(di) = deleted_idx {
        out.retain(|r| !r[di].is_truthy());
    }
    Ok(out)
}

/// Sums numeric columns (all of them, or just `sum_columns` if given) that
/// aren't part of the `ORDER BY` key, grouped by that key; other columns
/// keep the first row's value, the way ClickHouse picks an arbitrary one.
fn summing_final(
    order_by: &[String],
    sum_columns: Option<&[String]>,
    columns: &Columns,
    rows: &Rows,
) -> Result<Rows, ChError> {
    if order_by.is_empty() {
        return Ok(rows.clone());
    }
    let key_idxs: Vec<usize> =
        order_by.iter().map(|n| column_index(columns, n)).collect::<Result<_, _>>()?;
    let sum_idxs: Vec<usize> = match sum_columns {
        Some(names) => names.iter().map(|n| column_index(columns, n)).collect::<Result<_, _>>()?,
        None => columns
            .iter()
            .enumerate()
            .filter(|(i, (_, t))| !key_idxs.contains(i) && (t.is_integer() || t.is_float()))
            .map(|(i, _)| i)
            .collect(),
    };

    let mut pos_of: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut out: Rows = Vec::new();
    for row in rows {
        let key = group_key_text(row, &key_idxs);
        match pos_of.get(&key) {
            Some(&pos) => {
                for &si in &sum_idxs {
                    let a = out[pos][si].as_f64().unwrap_or(0.0);
                    let b = row[si].as_f64().unwrap_or(0.0);
                    out[pos][si] = match &out[pos][si] {
                        Val::UInt(_) => Val::UInt((a + b) as u64),
                        Val::Int(_) => Val::Int((a + b) as i64),
                        _ => Val::Float(a + b),
                    };
                }
            }
            None => {
                pos_of.insert(key, out.len());
                out.push(row.clone());
            }
        }
    }
    Ok(out)
}

// ---- SELECT ------------------------------------------------------------

/// A real table's engine and `ORDER BY`, so `FINAL` can be applied. `None`
/// for `system.*` and the `FROM`-less scalar case, where `FINAL` doesn't
/// apply.
type TableMeta = Option<(catalog::Engine, Vec<String>)>;

fn base_rows(catalog: &Catalog, table: &Table) -> Result<(Columns, Rows, TableMeta), ChError> {
    match table {
        Table::None => Ok((vec![], vec![vec![]], None)),
        Table::SystemOne => {
            Ok((vec![("dummy".into(), Type::UInt8)], vec![vec![Val::UInt(0)]], None))
        }
        Table::SystemNumbers(n) => {
            let rows = (0..*n).map(|i| vec![Val::UInt(i)]).collect();
            Ok((vec![("number".into(), Type::UInt64)], rows, None))
        }
        Table::SystemTables => {
            let columns = vec![
                ("database".into(), Type::String),
                ("name".into(), Type::String),
                ("engine".into(), Type::String),
                ("total_rows".into(), Type::UInt64),
            ];
            let rows = catalog
                .list()
                .into_iter()
                .map(|(db, name, engine, rows)| {
                    vec![
                        Val::Str(db),
                        Val::Str(name),
                        Val::Str(engine.to_string()),
                        Val::UInt(rows as u64),
                    ]
                })
                .collect();
            Ok((columns, rows, None))
        }
        Table::Named { database, table } => {
            let database = database.clone().unwrap_or_else(|| "default".into());
            let t = catalog
                .get(&database, table)
                .ok_or_else(|| ChError::unknown_table(&database, table))?;
            Ok((t.columns.clone(), t.rows.clone(), Some((t.engine.clone(), t.order_by.clone()))))
        }
    }
}

fn run_select(catalog: &Catalog, s: &Select) -> Result<QueryResult, ChError> {
    let (columns, mut rows, meta) = base_rows(catalog, &s.table)?;
    if s.select_final
        && let Some((engine, order_by)) = &meta
    {
        rows = merge_final(engine, order_by, &columns, &rows)?;
    }
    transform_select(s, &columns, rows)
}

/// `WHERE` → aggregation-or-projection → `ORDER BY` → `LIMIT`. Shared by
/// `run_select` and materialized views, which run the same pipeline over
/// just the newly inserted block instead of a table's full rows.
fn transform_select(s: &Select, columns: &Columns, mut rows: Rows) -> Result<QueryResult, ChError> {
    if let Some(w) = &s.where_ {
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            if eval_row_expr(w, &row, columns)?.1.is_truthy() {
                kept.push(row);
            }
        }
        rows = kept;
    }

    let mut result = if is_aggregate_query(s) {
        run_aggregate(s, columns, rows)?
    } else {
        run_projection(s, columns, rows)?
    };

    if !s.order_by.is_empty() {
        sort_rows(&mut result, &s.order_by)?;
    }
    Ok(limited(result, s.limit))
}

fn limited(mut r: QueryResult, limit: Option<u64>) -> QueryResult {
    if let Some(n) = limit {
        r.rows.truncate(n as usize);
    }
    r
}

fn sort_rows(result: &mut QueryResult, order_by: &[(Expr, bool)]) -> Result<(), ChError> {
    let mut keys = Vec::with_capacity(order_by.len());
    for (e, desc) in order_by {
        let name = e.default_name();
        let idx =
            result.columns.iter().position(|(n, _)| *n == name).ok_or_else(|| {
                ChError::not_implemented(&format!("ORDER BY {name} (not selected)"))
            })?;
        keys.push((idx, *desc));
    }
    result.rows.sort_by(|a, b| {
        for &(idx, desc) in &keys {
            let ord = compare(&a[idx], &b[idx]).unwrap_or(std::cmp::Ordering::Equal);
            let ord = if desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

/// Projects `select`'s item list onto rows with no aggregation: `*` returns
/// every column, other expressions are evaluated per row.
fn run_projection(
    s: &Select,
    columns: &[(String, Type)],
    rows: Vec<Vec<Val>>,
) -> Result<QueryResult, ChError> {
    if let [item] = s.items.as_slice()
        && item.expr == Expr::Star
        && item.alias.is_none()
    {
        return Ok(QueryResult { columns: columns.to_vec(), rows });
    }
    // A probe row (columns' zero values) types the output even when `rows`
    // is empty.
    let probe: Vec<Val> = columns.iter().map(|(_, t)| types::zero_value(*t)).collect();
    let mut out_columns = Vec::with_capacity(s.items.len());
    for item in &s.items {
        if item.expr == Expr::Star {
            return Err(ChError::not_implemented("* mixed with other select items"));
        }
        let (ty, _) = eval_row_expr(&item.expr, &probe, columns)?;
        let name = item.alias.clone().unwrap_or_else(|| item.expr.default_name());
        out_columns.push((name, ty));
    }
    let mut out_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut out_row = Vec::with_capacity(s.items.len());
        for item in &s.items {
            out_row.push(eval_row_expr(&item.expr, row, columns)?.1);
        }
        out_rows.push(out_row);
    }
    Ok(QueryResult { columns: out_columns, rows: out_rows })
}

// ---- Aggregation ---------------------------------------------------------

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "any" | "uniqexact"
    )
}

fn contains_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Call(name, args) => is_aggregate_name(name) || args.iter().any(contains_aggregate),
        Expr::BinOp(_, l, r) => contains_aggregate(l) || contains_aggregate(r),
        Expr::Neg(x) | Expr::Not(x) => contains_aggregate(x),
        _ => false,
    }
}

fn is_aggregate_query(s: &Select) -> bool {
    !s.group_by.is_empty() || s.items.iter().any(|i| contains_aggregate(&i.expr))
}

fn run_aggregate(
    s: &Select,
    columns: &[(String, Type)],
    rows: Vec<Vec<Val>>,
) -> Result<QueryResult, ChError> {
    // group key (as a joined text form, since Val doesn't implement Hash) ->
    // group index.
    let mut groups: Vec<(Vec<Val>, Vec<usize>)> = Vec::new();
    if s.group_by.is_empty() {
        groups.push((vec![], (0..rows.len()).collect()));
    } else {
        let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (ri, row) in rows.iter().enumerate() {
            let mut key = Vec::with_capacity(s.group_by.len());
            for ge in &s.group_by {
                key.push(eval_row_expr(ge, row, columns)?.1);
            }
            let key_text = key.iter().map(val_text).collect::<Vec<_>>().join("\u{1}");
            match index.get(&key_text) {
                Some(&gi) => groups[gi].1.push(ri),
                None => {
                    index.insert(key_text, groups.len());
                    groups.push((key, vec![ri]));
                }
            }
        }
    }

    let mut out_columns: Vec<(String, Type)> = Vec::new();
    let mut out_rows = Vec::with_capacity(groups.len());
    for (key_vals, member_idxs) in &groups {
        let mut out_row = Vec::with_capacity(s.items.len());
        for (ii, item) in s.items.iter().enumerate() {
            let (ty, v) =
                eval_group_item(&item.expr, &s.group_by, key_vals, member_idxs, &rows, columns)?;
            if out_columns.len() <= ii {
                let name = item.alias.clone().unwrap_or_else(|| item.expr.default_name());
                out_columns.push((name, ty));
            }
            out_row.push(v);
        }
        out_rows.push(out_row);
    }
    // GROUP BY over zero source rows produces zero groups (and correctly
    // zero output rows) — but the header still needs types, worked out
    // against a probe row instead of any real group.
    if out_columns.is_empty() {
        let probe: Vec<Val> = columns.iter().map(|(_, t)| types::zero_value(*t)).collect();
        let mut probe_key = Vec::with_capacity(s.group_by.len());
        for ge in &s.group_by {
            probe_key.push(eval_row_expr(ge, &probe, columns)?.1);
        }
        for item in &s.items {
            let (ty, _) =
                eval_group_item(&item.expr, &s.group_by, &probe_key, &[], &rows, columns)?;
            let name = item.alias.clone().unwrap_or_else(|| item.expr.default_name());
            out_columns.push((name, ty));
        }
    }
    Ok(QueryResult { columns: out_columns, rows: out_rows })
}

fn eval_group_item(
    e: &Expr,
    group_by: &[Expr],
    key_vals: &[Val],
    member_idxs: &[usize],
    rows: &[Vec<Val>],
    columns: &[(String, Type)],
) -> Result<(Type, Val), ChError> {
    if let Some(pos) = group_by.iter().position(|g| g == e) {
        let ty = match member_idxs.first() {
            Some(&ri) => eval_row_expr(e, &rows[ri], columns)?.0,
            None => key_vals[pos].natural_type(),
        };
        return Ok((ty, key_vals[pos].clone()));
    }
    if let Expr::Call(name, args) = e
        && is_aggregate_name(name)
    {
        return eval_aggregate(name, args, member_idxs, rows, columns);
    }
    // A non-aggregate, non-group-key expression (a constant, or one built
    // purely from group-key columns): evaluate against a member row.
    match member_idxs.first() {
        Some(&ri) => eval_row_expr(e, &rows[ri], columns),
        None => {
            let probe: Vec<Val> = columns.iter().map(|(_, t)| types::zero_value(*t)).collect();
            eval_row_expr(e, &probe, columns)
        }
    }
}

fn eval_aggregate(
    name: &str,
    args: &[Expr],
    member_idxs: &[usize],
    rows: &[Vec<Val>],
    columns: &[(String, Type)],
) -> Result<(Type, Val), ChError> {
    let lname = name.to_ascii_lowercase();
    if lname == "count" {
        return Ok((Type::UInt64, Val::UInt(member_idxs.len() as u64)));
    }
    let arg = args.first().ok_or_else(|| ChError::syntax(format!("{name}() needs an argument")))?;
    let mut vals = Vec::with_capacity(member_idxs.len());
    for &ri in member_idxs {
        vals.push(eval_row_expr(arg, &rows[ri], columns)?.1);
    }
    match lname.as_str() {
        "sum" => {
            let total: f64 = vals.iter().filter_map(Val::as_f64).sum();
            Ok((Type::Float64, Val::Float(total)))
        }
        "avg" => {
            if vals.is_empty() {
                return Ok((Type::Float64, Val::Float(0.0)));
            }
            let total: f64 = vals.iter().filter_map(Val::as_f64).sum();
            Ok((Type::Float64, Val::Float(total / vals.len() as f64)))
        }
        "min" | "max" => {
            let mut best: Option<Val> = None;
            for v in vals {
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        let ord = compare(&v, &b).ok_or_else(|| ChError::type_mismatch(name))?;
                        let keep_new = (lname == "min" && ord == std::cmp::Ordering::Less)
                            || (lname == "max" && ord == std::cmp::Ordering::Greater);
                        if keep_new { v } else { b }
                    }
                });
            }
            match best {
                Some(v) => Ok((v.natural_type(), v)),
                None => Ok((Type::Float64, Val::Float(0.0))),
            }
        }
        "any" => match vals.into_iter().next() {
            Some(v) => Ok((v.natural_type(), v)),
            None => Ok((Type::Float64, Val::Float(0.0))),
        },
        "uniqexact" => {
            let set: HashSet<String> = vals.iter().map(val_text).collect();
            Ok((Type::UInt64, Val::UInt(set.len() as u64)))
        }
        _ => unreachable!("checked by is_aggregate_name"),
    }
}

// ---- Scalar expression evaluation --------------------------------------

fn val_text(v: &Val) -> String {
    match v {
        Val::UInt(n) => n.to_string(),
        Val::Int(n) => n.to_string(),
        Val::Float(f) => f.to_string(),
        Val::Str(s) => s.clone(),
        Val::Bool(b) => b.to_string(),
    }
}

fn compare(a: &Val, b: &Val) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Val::Str(x), Val::Str(y)) => Some(x.cmp(y)),
        (Val::Str(_), _) | (_, Val::Str(_)) => None,
        _ => a.as_f64()?.partial_cmp(&b.as_f64()?),
    }
}

/// Evaluates `e` against `row` (columns describe `row`'s schema). `row` and
/// `columns` are both empty for a scalar expression with no `FROM`.
fn eval_row_expr(
    e: &Expr,
    row: &[Val],
    columns: &[(String, Type)],
) -> Result<(Type, Val), ChError> {
    match e {
        Expr::Int(n) if (0..=255).contains(n) => Ok((Type::UInt8, Val::UInt(*n as u64))),
        Expr::Int(n) => Ok((Type::Int64, Val::Int(*n))),
        Expr::Float(f) => Ok((Type::Float64, Val::Float(*f))),
        Expr::Str(s) => Ok((Type::String, Val::Str(s.clone()))),
        Expr::Bool(b) => Ok((Type::Bool, Val::Bool(*b))),
        Expr::Ident(name) => {
            let idx = columns
                .iter()
                .position(|(n, _)| n == name)
                .ok_or_else(|| ChError::unknown_identifier(name))?;
            Ok((columns[idx].1, row[idx].clone()))
        }
        Expr::Star => Err(ChError::syntax("* is only valid as the sole select item")),
        Expr::Neg(inner) => {
            let (ty, v) = eval_row_expr(inner, row, columns)?;
            match v {
                Val::Int(n) => Ok((Type::Int64, Val::Int(-n))),
                Val::UInt(n) => Ok((Type::Int64, Val::Int(-(n as i64)))),
                Val::Float(f) => Ok((Type::Float64, Val::Float(-f))),
                _ => Err(ChError::type_mismatch(ty.name())),
            }
        }
        Expr::Not(inner) => {
            let (_, v) = eval_row_expr(inner, row, columns)?;
            Ok((Type::Bool, Val::Bool(!v.is_truthy())))
        }
        Expr::BinOp(op, l, r) => eval_binop(*op, l, r, row, columns),
        Expr::Call(name, args) => eval_call(name, args, row, columns),
    }
}

fn eval_binop(
    op: BinOp,
    l: &Expr,
    r: &Expr,
    row: &[Val],
    columns: &[(String, Type)],
) -> Result<(Type, Val), ChError> {
    let (lt, lv) = eval_row_expr(l, row, columns)?;
    let (rt, rv) = eval_row_expr(r, row, columns)?;
    match op {
        BinOp::And => Ok((Type::Bool, Val::Bool(lv.is_truthy() && rv.is_truthy()))),
        BinOp::Or => Ok((Type::Bool, Val::Bool(lv.is_truthy() || rv.is_truthy()))),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            let ord =
                compare(&lv, &rv).ok_or_else(|| ChError::no_common_type(lt.name(), rt.name()))?;
            use std::cmp::Ordering::*;
            let b = match op {
                BinOp::Eq => ord == Equal,
                BinOp::Ne => ord != Equal,
                BinOp::Lt => ord == Less,
                BinOp::Le => ord != Greater,
                BinOp::Gt => ord == Greater,
                BinOp::Ge => ord != Less,
                _ => unreachable!(),
            };
            Ok((Type::Bool, Val::Bool(b)))
        }
        BinOp::Div => {
            let a = lv.as_f64().ok_or_else(|| ChError::type_mismatch(lt.name()))?;
            let b = rv.as_f64().ok_or_else(|| ChError::type_mismatch(rt.name()))?;
            Ok((Type::Float64, Val::Float(a / b)))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod => {
            if lv.as_i128().is_none() || rv.as_i128().is_none() {
                let a = lv.as_f64().ok_or_else(|| ChError::type_mismatch(lt.name()))?;
                let b = rv.as_f64().ok_or_else(|| ChError::type_mismatch(rt.name()))?;
                let f = match op {
                    BinOp::Add => a + b,
                    BinOp::Sub => a - b,
                    BinOp::Mul => a * b,
                    BinOp::Mod => a % b,
                    _ => unreachable!(),
                };
                return Ok((Type::Float64, Val::Float(f)));
            }
            let a = lv.as_i128().unwrap();
            let b = rv.as_i128().unwrap();
            let n = match op {
                BinOp::Add => a + b,
                BinOp::Sub => a - b,
                BinOp::Mul => a * b,
                BinOp::Mod => {
                    if b == 0 {
                        return Err(ChError::division_by_zero());
                    }
                    a % b
                }
                _ => unreachable!(),
            };
            let ty = if (0..=255).contains(&n) { Type::UInt8 } else { Type::Int64 };
            let v = if n >= 0 { Val::UInt(n as u64) } else { Val::Int(n as i64) };
            Ok((ty, v))
        }
    }
}

fn eval_call(
    name: &str,
    args: &[Expr],
    row: &[Val],
    columns: &[(String, Type)],
) -> Result<(Type, Val), ChError> {
    let lname = name.to_ascii_lowercase();
    if args.is_empty()
        && let Some(r) = eval_zero_arg(&lname)
    {
        return Ok(r);
    }
    let evaled: Vec<(Type, Val)> =
        args.iter().map(|a| eval_row_expr(a, row, columns)).collect::<Result<_, _>>()?;
    let a = evaled.as_slice();
    match (lname.as_str(), a) {
        ("tostring", [(_, v)]) => Ok((Type::String, Val::Str(val_text(v)))),
        ("toint8", [(_, v)]) => to_int(v, Type::Int8),
        ("toint16", [(_, v)]) => to_int(v, Type::Int16),
        ("toint32", [(_, v)]) => to_int(v, Type::Int32),
        ("toint64", [(_, v)]) => to_int(v, Type::Int64),
        ("touint8", [(_, v)]) => to_int(v, Type::UInt8),
        ("touint16", [(_, v)]) => to_int(v, Type::UInt16),
        ("touint32", [(_, v)]) => to_int(v, Type::UInt32),
        ("touint64", [(_, v)]) => to_int(v, Type::UInt64),
        ("tofloat32", [(_, v)]) => Ok((
            Type::Float32,
            Val::Float(v.as_f64().ok_or_else(|| ChError::type_mismatch("Float32"))?),
        )),
        ("tofloat64", [(_, v)]) => Ok((
            Type::Float64,
            Val::Float(v.as_f64().ok_or_else(|| ChError::type_mismatch("Float64"))?),
        )),
        ("length", [(_, Val::Str(s))]) => Ok((Type::UInt64, Val::UInt(s.chars().count() as u64))),
        ("upper", [(_, Val::Str(s))]) => Ok((Type::String, Val::Str(s.to_uppercase()))),
        ("lower", [(_, Val::Str(s))]) => Ok((Type::String, Val::Str(s.to_lowercase()))),
        ("concat", args) => {
            Ok((Type::String, Val::Str(args.iter().map(|(_, v)| val_text(v)).collect())))
        }
        ("substring", [(_, Val::Str(s)), (_, off), (_, len)]) => {
            let chars: Vec<char> = s.chars().collect();
            let off = ((off.as_f64().unwrap_or(1.0) as i64).max(1) as usize - 1).min(chars.len());
            let len = len.as_f64().unwrap_or(0.0).max(0.0) as usize;
            let end = (off + len).min(chars.len());
            Ok((Type::String, Val::Str(chars[off..end].iter().collect())))
        }
        ("trim", [(_, Val::Str(s))]) => Ok((Type::String, Val::Str(s.trim().to_string()))),
        ("replaceall", [(_, Val::Str(s)), (_, Val::Str(from)), (_, Val::Str(to))]) => {
            Ok((Type::String, Val::Str(s.replace(from.as_str(), to))))
        }
        ("abs", [(ty, v)]) => match v {
            Val::Int(n) => Ok((*ty, Val::Int(n.abs()))),
            Val::UInt(n) => Ok((*ty, Val::UInt(*n))),
            Val::Float(f) => Ok((*ty, Val::Float(f.abs()))),
            _ => Err(ChError::type_mismatch("abs")),
        },
        ("round", [(_, v)]) => Ok((
            Type::Float64,
            Val::Float(v.as_f64().ok_or_else(|| ChError::type_mismatch("round"))?.round()),
        )),
        ("floor", [(_, v)]) => Ok((
            Type::Float64,
            Val::Float(v.as_f64().ok_or_else(|| ChError::type_mismatch("floor"))?.floor()),
        )),
        ("ceil", [(_, v)]) => Ok((
            Type::Float64,
            Val::Float(v.as_f64().ok_or_else(|| ChError::type_mismatch("ceil"))?.ceil()),
        )),
        ("greatest", args) if !args.is_empty() => {
            let vals: Option<Vec<f64>> = args.iter().map(|(_, v)| v.as_f64()).collect();
            let vals = vals.ok_or_else(|| ChError::type_mismatch("greatest"))?;
            Ok((Type::Float64, Val::Float(vals.into_iter().fold(f64::MIN, f64::max))))
        }
        ("least", args) if !args.is_empty() => {
            let vals: Option<Vec<f64>> = args.iter().map(|(_, v)| v.as_f64()).collect();
            let vals = vals.ok_or_else(|| ChError::type_mismatch("least"))?;
            Ok((Type::Float64, Val::Float(vals.into_iter().fold(f64::MAX, f64::min))))
        }
        ("if", [(_, cond), (t1, v1), (_, v2)]) => {
            Ok((*t1, if cond.is_truthy() { v1.clone() } else { v2.clone() }))
        }
        // No NULLs modeled yet (Nullable isn't a supported column type), so
        // the "or-null" behaviour these normally guard against never
        // triggers: the first argument always wins.
        ("ifnull", [(t, v), _]) => Ok((*t, v.clone())),
        ("coalesce", args) if !args.is_empty() => Ok((args[0].0, args[0].1.clone())),
        ("isnull", _) => Ok((Type::Bool, Val::Bool(false))),
        ("isnotnull", _) => Ok((Type::Bool, Val::Bool(true))),
        _ => Err(ChError::unknown_function(name)),
    }
}

fn to_int(v: &Val, t: Type) -> Result<(Type, Val), ChError> {
    let n = v
        .as_i128()
        .or_else(|| v.as_f64().map(|f| f as i128))
        .ok_or_else(|| ChError::type_mismatch(t.name()))?;
    let coerced = types::coerce(&if n >= 0 { Val::UInt(n as u64) } else { Val::Int(n as i64) }, t)?;
    Ok((t, coerced))
}

fn eval_zero_arg(lname: &str) -> Option<(Type, Val)> {
    match lname {
        "version" => Some((Type::String, Val::Str(SERVER_VERSION.into()))),
        "currentdatabase" => Some((Type::String, Val::Str("default".into()))),
        "hostname" => Some((Type::String, Val::Str("localhost".into()))),
        "timezone" => Some((Type::String, Val::Str("UTC".into()))),
        "uptime" => Some((Type::UInt64, Val::UInt(0))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> Engine {
        Engine::new()
    }

    fn run(e: &Engine, sql: &str) -> QueryResult {
        e.execute(sql, b"").unwrap_or_else(|err| panic!("{sql}: {err:?}")).0
    }

    fn run_sql(sql: &str) -> QueryResult {
        run(&engine(), sql)
    }

    #[test]
    fn select_one() {
        let r = run_sql("SELECT 1");
        assert_eq!(r.columns, [("1".to_string(), Type::UInt8)]);
        assert_eq!(r.rows, [[Val::UInt(1)]]);
    }

    #[test]
    fn select_large_int_is_int64() {
        let r = run_sql("SELECT 1000");
        assert_eq!(r.columns, [("1000".to_string(), Type::Int64)]);
    }

    #[test]
    fn select_version() {
        let r = run_sql("SELECT version()");
        assert_eq!(r.columns, [("version()".to_string(), Type::String)]);
        assert_eq!(r.rows, [[Val::Str(SERVER_VERSION.into())]]);
    }

    #[test]
    fn select_aliased() {
        let r = run_sql("SELECT 1 AS one");
        assert_eq!(r.columns, [("one".to_string(), Type::UInt8)]);
    }

    #[test]
    fn select_star_from_system_one() {
        let r = run_sql("SELECT * FROM system.one");
        assert_eq!(r.columns, [("dummy".to_string(), Type::UInt8)]);
        assert_eq!(r.rows, [[Val::UInt(0)]]);
    }

    #[test]
    fn select_from_numbers_with_limit() {
        let r = run_sql("SELECT number FROM numbers(100) LIMIT 3");
        assert_eq!(r.rows, [[Val::UInt(0)], [Val::UInt(1)], [Val::UInt(2)]]);
    }

    #[test]
    fn select_star_from_numbers() {
        let r = run_sql("SELECT * FROM numbers(5)");
        assert_eq!(r.rows.len(), 5);
    }

    #[test]
    fn unknown_table_errors() {
        let e = engine();
        let err = e.execute("SELECT * FROM nope", b"").unwrap_err();
        assert_eq!(err.code, 60);
    }

    #[test]
    fn unknown_function_errors() {
        let err = engine().execute("SELECT nosuchfn()", b"").unwrap_err();
        assert_eq!(err.code, 46);
    }

    #[test]
    fn bare_column_name_with_no_from_is_unknown_identifier() {
        let err = engine().execute("SELECT FROM", b"").unwrap_err();
        assert_eq!(err.code, 47);
    }

    #[test]
    fn syntax_error_propagates() {
        let err = engine().execute("not sql", b"").unwrap_err();
        assert_eq!(err.code, 62);
    }

    #[test]
    fn format_clause_is_returned() {
        let (_, format) = engine().execute("SELECT 1 FORMAT JSON", b"").unwrap();
        assert_eq!(format, Some("JSON".into()));
    }

    #[test]
    fn arithmetic_and_comparisons() {
        let r = run_sql("SELECT 1 + 2 * 3, 7 / 2, 7 % 2, 1 < 2, 1 = 1 AND 2 = 3");
        assert_eq!(r.rows[0][0], Val::UInt(7));
        assert_eq!(r.rows[0][1], Val::Float(3.5));
        assert_eq!(r.rows[0][2], Val::UInt(1));
        assert_eq!(r.rows[0][3], Val::Bool(true));
        assert_eq!(r.rows[0][4], Val::Bool(false));
    }

    #[test]
    fn create_insert_select_round_trip() {
        let e = engine();
        run(&e, "CREATE TABLE t (id UInt32, name String) ENGINE = Memory");
        run(&e, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')");
        let r = run(&e, "SELECT * FROM t");
        assert_eq!(
            r.columns,
            [("id".to_string(), Type::UInt32), ("name".to_string(), Type::String)]
        );
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn create_table_twice_errors_without_if_not_exists() {
        let e = engine();
        run(&e, "CREATE TABLE t (id UInt32) ENGINE = Memory");
        let err = e.execute("CREATE TABLE t (id UInt32) ENGINE = Memory", b"").unwrap_err();
        assert_eq!(err.code, 57);
        // IF NOT EXISTS makes the same statement a no-op.
        run(&e, "CREATE TABLE IF NOT EXISTS t (id UInt32) ENGINE = Memory");
    }

    #[test]
    fn insert_into_missing_table_errors() {
        let err = engine().execute("INSERT INTO nope VALUES (1)", b"").unwrap_err();
        assert_eq!(err.code, 60);
    }

    #[test]
    fn insert_range_checks_declared_type() {
        let e = engine();
        run(&e, "CREATE TABLE t (n UInt8) ENGINE = Memory");
        let err = e.execute("INSERT INTO t VALUES (300)", b"").unwrap_err();
        assert_eq!(err.code, 69);
    }

    #[test]
    fn insert_defaults_unmentioned_columns() {
        let e = engine();
        run(&e, "CREATE TABLE t (id UInt32, name String) ENGINE = Memory");
        run(&e, "INSERT INTO t (id) VALUES (1)");
        let r = run(&e, "SELECT id, name FROM t");
        assert_eq!(r.rows, [[Val::UInt(1), Val::Str(String::new())]]);
    }

    #[test]
    fn where_filters_rows() {
        let e = engine();
        run(&e, "CREATE TABLE t (n UInt32) ENGINE = Memory");
        run(&e, "INSERT INTO t VALUES (1), (2), (3)");
        let r = run(&e, "SELECT n FROM t WHERE n > 1");
        assert_eq!(r.rows, [[Val::UInt(2)], [Val::UInt(3)]]);
    }

    #[test]
    fn group_by_and_aggregates() {
        let e = engine();
        run(&e, "CREATE TABLE t (k String, v UInt32) ENGINE = Memory");
        run(&e, "INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10)");
        let r = run(&e, "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k");
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0], [Val::Str("a".into()), Val::UInt(2), Val::Float(3.0)]);
        assert_eq!(r.rows[1], [Val::Str("b".into()), Val::UInt(1), Val::Float(10.0)]);
    }

    #[test]
    fn count_without_group_by_is_one_implicit_group() {
        let e = engine();
        run(&e, "CREATE TABLE t (n UInt32) ENGINE = Memory");
        run(&e, "INSERT INTO t VALUES (1), (2), (3)");
        let r = run(&e, "SELECT count(*), sum(n), avg(n), min(n), max(n) FROM t");
        assert_eq!(
            r.rows[0],
            [Val::UInt(3), Val::Float(6.0), Val::Float(2.0), Val::UInt(1), Val::UInt(3)]
        );
    }

    #[test]
    fn order_by_desc_and_limit() {
        let e = engine();
        run(&e, "CREATE TABLE t (n UInt32) ENGINE = Memory");
        run(&e, "INSERT INTO t VALUES (3), (1), (2)");
        let r = run(&e, "SELECT n FROM t ORDER BY n DESC LIMIT 2");
        assert_eq!(r.rows, [[Val::UInt(3)], [Val::UInt(2)]]);
    }

    #[test]
    fn drop_table_then_select_is_unknown_table() {
        let e = engine();
        run(&e, "CREATE TABLE t (n UInt32) ENGINE = Memory");
        run(&e, "DROP TABLE t");
        let err = e.execute("SELECT * FROM t", b"").unwrap_err();
        assert_eq!(err.code, 60);
        // IF EXISTS makes dropping an already-gone table a no-op.
        run(&e, "DROP TABLE IF EXISTS t");
    }

    #[test]
    fn system_tables_lists_created_tables() {
        let e = engine();
        run(&e, "CREATE TABLE t (n UInt32) ENGINE = MergeTree() ORDER BY (n)");
        let r = run(&e, "SELECT name, engine FROM system.tables");
        assert_eq!(r.rows, [[Val::Str("t".into()), Val::Str("MergeTree".into())]]);
    }

    #[test]
    fn string_and_math_functions() {
        let r = run_sql("SELECT upper('ab'), length('abc'), abs(-5), round(1.6), toString(42)");
        assert_eq!(r.rows[0][0], Val::Str("AB".into()));
        assert_eq!(r.rows[0][1], Val::UInt(3));
        assert_eq!(r.rows[0][2], Val::Int(5));
        assert_eq!(r.rows[0][3], Val::Float(2.0));
        assert_eq!(r.rows[0][4], Val::Str("42".into()));
    }

    #[test]
    fn to_int_range_checks() {
        let err = engine().execute("SELECT toUInt8(300)", b"").unwrap_err();
        assert_eq!(err.code, 69);
    }

    #[test]
    fn replacing_merge_tree_final_dedups_by_order_by_and_ver() {
        let e = engine();
        run(
            &e,
            "CREATE TABLE t (id UInt32, v String, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY (id)",
        );
        run(&e, "INSERT INTO t VALUES (1, 'old', 1), (1, 'new', 2), (2, 'x', 1)");
        let r = run(&e, "SELECT id, v FROM t FINAL ORDER BY id");
        assert_eq!(
            r.rows,
            [[Val::UInt(1), Val::Str("new".into())], [Val::UInt(2), Val::Str("x".into())]]
        );
        // Without FINAL, both rows for id=1 are still there.
        let r = run(&e, "SELECT count(*) FROM t");
        assert_eq!(r.rows, [[Val::UInt(3)]]);
    }

    #[test]
    fn replacing_merge_tree_without_ver_keeps_last_inserted() {
        let e = engine();
        run(&e, "CREATE TABLE t (id UInt32, v String) ENGINE = ReplacingMergeTree ORDER BY (id)");
        run(&e, "INSERT INTO t VALUES (1, 'first')");
        run(&e, "INSERT INTO t VALUES (1, 'second')");
        let r = run(&e, "SELECT v FROM t FINAL");
        assert_eq!(r.rows, [[Val::Str("second".into())]]);
    }

    #[test]
    fn replacing_merge_tree_is_deleted_drops_rows() {
        let e = engine();
        run(
            &e,
            "CREATE TABLE t (id UInt32, deleted Bool) ENGINE = ReplacingMergeTree(id, deleted) ORDER BY (id)",
        );
        run(&e, "INSERT INTO t VALUES (1, false)");
        run(&e, "INSERT INTO t VALUES (1, true)");
        let r = run(&e, "SELECT * FROM t FINAL");
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    fn summing_merge_tree_final_sums_numeric_columns() {
        let e = engine();
        run(&e, "CREATE TABLE t (k String, amount UInt32) ENGINE = SummingMergeTree ORDER BY (k)");
        run(&e, "INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10)");
        let r = run(&e, "SELECT k, amount FROM t FINAL ORDER BY k");
        assert_eq!(
            r.rows,
            [[Val::Str("a".into()), Val::UInt(3)], [Val::Str("b".into()), Val::UInt(10)]]
        );
    }

    #[test]
    fn optimize_table_final_persists_the_merge() {
        let e = engine();
        run(&e, "CREATE TABLE t (k String, amount UInt32) ENGINE = SummingMergeTree ORDER BY (k)");
        run(&e, "INSERT INTO t VALUES ('a', 1), ('a', 2)");
        run(&e, "OPTIMIZE TABLE t FINAL");
        // Now merged even without FINAL, since OPTIMIZE rewrote storage.
        let r = run(&e, "SELECT amount FROM t");
        assert_eq!(r.rows, [[Val::UInt(3)]]);
    }

    #[test]
    fn optimize_without_final_is_a_no_op() {
        let e = engine();
        run(&e, "CREATE TABLE t (k String, amount UInt32) ENGINE = SummingMergeTree ORDER BY (k)");
        run(&e, "INSERT INTO t VALUES ('a', 1), ('a', 2)");
        run(&e, "OPTIMIZE TABLE t");
        let r = run(&e, "SELECT count(*) FROM t");
        assert_eq!(r.rows, [[Val::UInt(2)]]);
    }

    #[test]
    fn materialized_view_to_form_aggregates_on_insert() {
        let e = engine();
        run(&e, "CREATE TABLE src (k String, v UInt32) ENGINE = Memory");
        run(
            &e,
            "CREATE TABLE target (k String, total UInt64) ENGINE = SummingMergeTree ORDER BY (k)",
        );
        run(
            &e,
            "CREATE MATERIALIZED VIEW mv TO target AS SELECT k, sum(v) AS total FROM src GROUP BY k",
        );
        run(&e, "INSERT INTO src VALUES ('a', 1), ('a', 2), ('b', 10)");
        let r = run(&e, "SELECT k, total FROM target FINAL ORDER BY k");
        assert_eq!(
            r.rows,
            [[Val::Str("a".into()), Val::UInt(3)], [Val::Str("b".into()), Val::UInt(10)]]
        );
        // A second insert only processes the new block, not the whole table.
        run(&e, "INSERT INTO src VALUES ('a', 100)");
        let r = run(&e, "SELECT k, total FROM target FINAL ORDER BY k");
        assert_eq!(
            r.rows,
            [[Val::Str("a".into()), Val::UInt(103)], [Val::Str("b".into()), Val::UInt(10)]]
        );
    }

    #[test]
    fn materialized_view_target_must_already_exist() {
        let e = engine();
        run(&e, "CREATE TABLE src (k String) ENGINE = Memory");
        let err =
            e.execute("CREATE MATERIALIZED VIEW mv TO nope AS SELECT k FROM src", b"").unwrap_err();
        assert_eq!(err.code, 60);
    }

    #[test]
    fn insert_via_row_binary_with_names_and_types() {
        let e = engine();
        run(&e, "CREATE TABLE t (id UInt32, name String) ENGINE = Memory");
        let payload = crate::clickhouse::engine::QueryResult {
            columns: vec![("id".to_string(), Type::UInt32), ("name".to_string(), Type::String)],
            rows: vec![
                vec![Val::UInt(1), Val::Str("a".into())],
                vec![Val::UInt(2), Val::Str("b".into())],
            ],
        };
        let body = crate::clickhouse::rowbinary::encode(&payload, true, true).unwrap();
        e.execute("INSERT INTO t FORMAT RowBinaryWithNamesAndTypes", &body).unwrap();
        let r = run(&e, "SELECT id, name FROM t ORDER BY id");
        assert_eq!(
            r.rows,
            [[Val::UInt(1), Val::Str("a".into())], [Val::UInt(2), Val::Str("b".into())]]
        );
    }
}
