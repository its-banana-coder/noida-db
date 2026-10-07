//! `EXPLAIN`: a plan shaped like the one Postgres would print, in the
//! `TEXT`, `JSON`, `XML` and `YAML` formats.
//!
//! noida-db has no cost-based planner, so the tree is read off the query:
//! a scan per relation, a join per JOIN, then aggregate, sort, limit and
//! row locks on top, the way Postgres stacks them. Row estimates are the
//! tables' real row counts; costs are derived from them. Tools that parse
//! the output (ORMs' `.explain()`, visualisers) get a well-formed document
//! in the format they asked for.

use sqlparser::ast as a;

/// `EXPLAIN (...)` options that change the output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Format {
    Text,
    Json,
    Xml,
    Yaml,
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub format: Format,
    pub analyze: bool,
    pub costs: bool,
    pub verbose: bool,
    pub summary: bool,
    pub timing: bool,
}

/// What ANALYZE measured.
pub struct Actual {
    pub rows: u64,
    pub planning_ms: f64,
    pub execution_ms: f64,
}

#[derive(Clone, Debug)]
enum Prop {
    Text(String),
    List(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct Node {
    kind: String,
    /// "Outer" / "Inner" / "Member" / "Subquery" under a parent.
    parent_rel: Option<&'static str>,
    join_type: Option<String>,
    relation: Option<(String, String)>,
    operation: Option<&'static str>,
    strategy: Option<&'static str>,
    rows: u64,
    width: u32,
    /// Gets ANALYZE's measured row count (the node a ModifyTable reads).
    measured: bool,
    props: Vec<(String, Prop)>,
    children: Vec<Node>,
}

impl Node {
    fn new(kind: &str, rows: u64) -> Node {
        Node {
            kind: kind.into(),
            parent_rel: None,
            join_type: None,
            relation: None,
            operation: None,
            strategy: None,
            rows,
            width: 32,
            measured: false,
            props: vec![],
            children: vec![],
        }
    }

    fn with_child(mut self, mut c: Node, rel: &'static str) -> Node {
        c.parent_rel = Some(rel);
        c.measured = self.kind == "ModifyTable";
        self.children.push(c);
        self
    }

    fn prop(mut self, k: &str, v: String) -> Node {
        self.props.push((k.into(), Prop::Text(v)));
        self
    }

    fn list(mut self, k: &str, v: Vec<String>) -> Node {
        self.props.push((k.into(), Prop::List(v)));
        self
    }

    /// A filter goes on a scan; above anything else it becomes the node's
    /// `Join Filter` / `Filter`.
    fn filter(self, cond: &a::Expr) -> Node {
        let key = if self.join_type.is_some() { "Join Filter" } else { "Filter" };
        let rows = (self.rows / 2).max(1);
        let mut n = self.prop(key, paren(cond));
        n.rows = rows;
        n
    }

    /// Total cost: a stable function of the rows below.
    fn total_cost(&self) -> f64 {
        let own = self.rows as f64 * 0.01 + 1.0;
        own + self.children.iter().map(Node::total_cost).sum::<f64>()
    }

    fn startup_cost(&self) -> f64 {
        match self.kind.as_str() {
            "Sort" | "Aggregate" | "HashAggregate" | "Hash" | "Unique" => {
                self.children.iter().map(Node::total_cost).sum::<f64>()
            }
            _ => 0.0,
        }
    }

    /// `Seq Scan on t x` as TEXT names it.
    fn label(&self) -> String {
        let mut s = match (self.strategy, self.kind.as_str()) {
            (Some("Hashed"), "Aggregate") => "HashAggregate".to_string(),
            (Some("Sorted"), "Aggregate") => "GroupAggregate".to_string(),
            _ => self.kind.clone(),
        };
        if let Some(j) = &self.join_type
            && j != "Inner"
        {
            // "Hash Left Join", "Nested Loop Anti Join".
            s = match self.kind.as_str() {
                "Hash Join" => format!("Hash {j} Join"),
                "Merge Join" => format!("Merge {j} Join"),
                _ => format!("{} {j} Join", self.kind),
            };
        }
        if let Some(op) = self.operation {
            s = op.to_string();
        }
        if let Some((rel, alias)) = &self.relation {
            s.push_str(&format!(" on {rel}"));
            if alias != rel {
                s.push_str(&format!(" {alias}"));
            }
        }
        s
    }
}

fn paren(e: &a::Expr) -> String {
    let s = e.to_string();
    if s.starts_with('(') && s.ends_with(')') { s } else { format!("({s})") }
}

/// Row counts of the tables a query names.
pub trait Rows {
    fn rows(&self, table: &str) -> u64;
}

impl<F: Fn(&str) -> u64> Rows for F {
    fn rows(&self, table: &str) -> u64 {
        self(table)
    }
}

pub fn plan(stmt: &a::Statement, db: &dyn Rows) -> Node {
    match stmt {
        a::Statement::Query(q) => query(q, db),
        a::Statement::Insert(ins) => {
            let rel = match &ins.table {
                a::TableObject::TableName(n) => last(n),
                other => other.to_string(),
            };
            let src = match &ins.source {
                Some(q) => query(q, db),
                None => Node::new("Result", 1),
            };
            let mut m = Node::new("ModifyTable", 0);
            m.operation = Some("Insert");
            m.relation = Some((
                rel.clone(),
                alias_or(&ins.table_alias.as_ref().map(|a| a.alias.value.clone()), &rel),
            ));
            m.with_child(src, "Outer")
        }
        a::Statement::Update(u) => {
            let (rel, alias) = relation_of(&u.table.relation);
            let mut scan = scan(&u.table.relation, db);
            if let Some(w) = &u.selection {
                scan = scan.filter(w);
            }
            let mut m = Node::new("ModifyTable", 0);
            m.operation = Some("Update");
            m.relation = Some((rel, alias));
            m.with_child(scan, "Outer")
        }
        a::Statement::Delete(d) => {
            let tables = match &d.from {
                a::FromTable::WithFromKeyword(t) | a::FromTable::WithoutKeyword(t) => t,
            };
            let Some(t) = tables.first() else { return Node::new("Result", 1) };
            let (rel, alias) = relation_of(&t.relation);
            let mut scan = scan(&t.relation, db);
            if let Some(w) = &d.selection {
                scan = scan.filter(w);
            }
            let mut m = Node::new("ModifyTable", 0);
            m.operation = Some("Delete");
            m.relation = Some((rel, alias));
            m.with_child(scan, "Outer")
        }
        _ => Node::new("Result", 1),
    }
}

fn alias_or(alias: &Option<String>, rel: &str) -> String {
    alias.clone().unwrap_or_else(|| rel.to_string())
}

fn last(n: &a::ObjectName) -> String {
    n.0.last().map(|p| p.to_string().trim_matches('"').to_string()).unwrap_or_default()
}

fn relation_of(f: &a::TableFactor) -> (String, String) {
    match f {
        a::TableFactor::Table { name, alias, .. } => {
            let rel = last(name);
            (rel.clone(), alias_or(&alias.as_ref().map(|a| a.name.value.clone()), &rel))
        }
        other => {
            let s = other.to_string();
            (s.clone(), s)
        }
    }
}

fn query(q: &a::Query, db: &dyn Rows) -> Node {
    let mut n = set_expr(&q.body, db);
    if let Some(ob) = &q.order_by
        && let a::OrderByKind::Expressions(keys) = &ob.kind
        && !keys.is_empty()
    {
        let rows = n.rows;
        n = Node::new("Sort", rows)
            .list("Sort Key", keys.iter().map(|k| k.to_string()).collect())
            .with_child(n, "Outer");
    }
    if !q.locks.is_empty() {
        let rows = n.rows;
        n = Node::new("LockRows", rows).with_child(n, "Outer");
    }
    if let Some(a::LimitClause::LimitOffset { limit, offset, .. }) = &q.limit_clause
        && (limit.is_some() || offset.is_some())
    {
        let rows = match limit {
            Some(a::Expr::Value(v)) => v.to_string().parse::<u64>().unwrap_or(n.rows).min(n.rows),
            _ => n.rows,
        };
        n = Node::new("Limit", rows).with_child(n, "Outer");
    }
    if let Some(with) = &q.with {
        for cte in &with.cte_tables {
            let mut c = query(&cte.query, db);
            c.parent_rel = Some("InitPlan");
            c = c.prop("Subplan Name", format!("CTE {}", cte.alias.name.value));
            n.children.push(c);
        }
    }
    n
}

fn set_expr(e: &a::SetExpr, db: &dyn Rows) -> Node {
    match e {
        a::SetExpr::Select(s) => select(s, db),
        a::SetExpr::Query(q) => query(q, db),
        a::SetExpr::SetOperation { op, set_quantifier, .. } => {
            let mut members = vec![];
            flatten(e, op, set_quantifier, &mut members);
            let parts: Vec<Node> = members.into_iter().map(|m| set_expr(m, db)).collect();
            let rows: u64 = parts.iter().map(|p| p.rows).sum();
            let all = matches!(set_quantifier, a::SetQuantifier::All);
            match op {
                a::SetOperator::Union => {
                    let mut app = Node::new("Append", rows);
                    for p in parts {
                        app = app.with_child(p, "Member");
                    }
                    if all {
                        app
                    } else {
                        let mut agg = Node::new("Aggregate", rows);
                        agg.strategy = Some("Hashed");
                        agg.with_child(app, "Outer")
                    }
                }
                _ => {
                    let cmd =
                        if matches!(op, a::SetOperator::Except) { "Except" } else { "Intersect" };
                    let mut app = Node::new("Append", rows);
                    for p in parts {
                        app = app.with_child(p, "Member");
                    }
                    let mut s = Node::new("SetOp", rows)
                        .prop("Command", if all { format!("{cmd} All") } else { cmd.to_string() });
                    s.strategy = Some("Hashed");
                    s.with_child(app, "Outer")
                }
            }
        }
        a::SetExpr::Values(v) => {
            Node::new("Values Scan", v.rows.len() as u64).prop("Alias", "\"*VALUES*\"".to_string())
        }
        _ => Node::new("Result", 1),
    }
}

/// `a UNION b UNION c` is one Append of three members in Postgres.
fn flatten<'q>(
    e: &'q a::SetExpr,
    op: &a::SetOperator,
    q: &a::SetQuantifier,
    out: &mut Vec<&'q a::SetExpr>,
) {
    match e {
        a::SetExpr::SetOperation { left, op: o, set_quantifier, right }
            if o == op && set_quantifier == q =>
        {
            flatten(left, op, q, out);
            flatten(right, op, q, out);
        }
        other => out.push(other),
    }
}

fn select(s: &a::Select, db: &dyn Rows) -> Node {
    let mut n = match s.from.len() {
        0 => Node::new("Result", 1),
        _ => {
            let mut it = s.from.iter().map(|t| table_with_joins(t, db));
            let first = it.next().unwrap();
            it.fold(first, |acc, r| {
                let rows = acc.rows.max(1) * r.rows.max(1);
                let mut nl = Node::new("Nested Loop", rows);
                nl.join_type = Some("Inner".into());
                nl.with_child(acc, "Outer").with_child(r, "Inner")
            })
        }
    };
    if let Some(w) = &s.selection {
        if s.from.is_empty() {
            n = n.prop("One-Time Filter", paren(w));
        } else {
            n = n.filter(w);
        }
    }
    let group: Vec<String> = match &s.group_by {
        a::GroupByExpr::Expressions(e, _) => e.iter().map(|x| x.to_string()).collect(),
        a::GroupByExpr::All(_) => vec![],
    };
    let aggregated = !group.is_empty()
        || s.having.is_some()
        || s.projection.iter().any(|p| {
            let p = p.to_string().to_lowercase();
            [
                "count(",
                "sum(",
                "avg(",
                "min(",
                "max(",
                "array_agg(",
                "string_agg(",
                "json_agg(",
                "jsonb_agg(",
                "bool_and(",
                "bool_or(",
            ]
            .iter()
            .any(|f| p.contains(f))
        });
    if aggregated {
        let rows = if group.is_empty() { 1 } else { (n.rows / 2).max(1) };
        let mut agg = Node::new("Aggregate", rows);
        agg.strategy = Some(if group.is_empty() { "Plain" } else { "Hashed" });
        if !group.is_empty() {
            agg = agg.list("Group Key", group);
        }
        if let Some(h) = &s.having {
            agg = agg.prop("Filter", paren(h));
        }
        n = agg.with_child(n, "Outer");
    }
    match &s.distinct {
        Some(a::Distinct::Distinct) => {
            let rows = n.rows;
            let keys = s.projection.iter().map(|p| p.to_string()).collect();
            let mut agg = Node::new("Aggregate", rows).list("Group Key", keys);
            agg.strategy = Some("Hashed");
            n = agg.with_child(n, "Outer");
        }
        Some(a::Distinct::On(on)) => {
            let rows = n.rows;
            let keys: Vec<String> = on.iter().map(|e| e.to_string()).collect();
            let sort = Node::new("Sort", rows).list("Sort Key", keys).with_child(n, "Outer");
            n = Node::new("Unique", rows).with_child(sort, "Outer");
        }
        _ => {}
    }
    n
}

fn table_with_joins(t: &a::TableWithJoins, db: &dyn Rows) -> Node {
    let mut n = scan(&t.relation, db);
    for j in &t.joins {
        let right = scan(&j.relation, db);
        use a::JoinOperator as J;
        let (jt, cons) = match &j.join_operator {
            J::Join(c) | J::Inner(c) => ("Inner", Some(c)),
            J::Left(c) | J::LeftOuter(c) => ("Left", Some(c)),
            J::Right(c) | J::RightOuter(c) => ("Right", Some(c)),
            J::FullOuter(c) => ("Full", Some(c)),
            J::CrossJoin(c) => ("Inner", Some(c)),
            J::Semi(c) | J::LeftSemi(c) | J::RightSemi(c) => ("Semi", Some(c)),
            J::Anti(c) | J::LeftAnti(c) | J::RightAnti(c) => ("Anti", Some(c)),
            _ => ("Inner", None),
        };
        let rows = n.rows.max(right.rows).max(1);
        let cond = match cons {
            Some(a::JoinConstraint::On(e)) => Some(paren(e)),
            Some(a::JoinConstraint::Using(cols)) => Some(format!(
                "({})",
                cols.iter()
                    .map(|c| {
                        let c = last(c);
                        format!(
                            "{l}.{c} = {r}.{c}",
                            l = alias_name(&t.relation),
                            r = alias_name(&j.relation)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ")
            )),
            _ => None,
        };
        let join = match &cond {
            Some(c) => {
                let mut h = Node::new("Hash Join", rows);
                h.join_type = Some(jt.into());
                let hash = Node::new("Hash", right.rows).with_child(right, "Outer");
                h = h.prop("Hash Cond", c.clone());
                h.with_child(n, "Outer").with_child(hash, "Inner")
            }
            None => {
                let mut nl = Node::new("Nested Loop", rows);
                nl.join_type = Some(jt.into());
                nl.with_child(n, "Outer").with_child(right, "Inner")
            }
        };
        n = join;
    }
    n
}

fn alias_name(f: &a::TableFactor) -> String {
    relation_of(f).1
}

fn scan(f: &a::TableFactor, db: &dyn Rows) -> Node {
    match f {
        a::TableFactor::Table { name, alias, args: Some(_), .. } => {
            let fname = last(name);
            let mut n = Node::new("Function Scan", 1000);
            n = n.prop("Function Name", fname.clone());
            n.prop("Alias", alias_or(&alias.as_ref().map(|a| a.name.value.clone()), &fname))
        }
        a::TableFactor::Table { name, alias, .. } => {
            let rel = last(name);
            let mut n = Node::new("Seq Scan", db.rows(&rel));
            n.relation =
                Some((rel.clone(), alias_or(&alias.as_ref().map(|a| a.name.value.clone()), &rel)));
            n
        }
        a::TableFactor::Derived { subquery, alias, .. } => {
            let inner = query(subquery, db);
            let rows = inner.rows;
            let mut n = Node::new("Subquery Scan", rows).with_child(inner, "Subquery");
            if let Some(al) = alias {
                n = n.prop("Alias", al.name.value.clone());
            }
            n
        }
        a::TableFactor::NestedJoin { table_with_joins: t, .. } => table_with_joins(t, db),
        _ => Node::new("Function Scan", 1000),
    }
}

// ---------------------------------------------------------------------------
// Output

/// The `QUERY PLAN` rows: one per line in TEXT, one document otherwise.
pub fn render(root: &Node, o: &Options, actual: Option<&Actual>) -> Vec<String> {
    match o.format {
        Format::Text => {
            let mut lines = vec![];
            text(root, 0, o, actual, &mut lines);
            summary_text(o, actual, &mut lines);
            lines
        }
        Format::Json => vec![json_doc(root, o, actual)],
        Format::Xml => vec![xml_doc(root, o, actual)],
        Format::Yaml => vec![yaml_doc(root, o, actual)],
    }
}

fn fmt2(x: f64) -> String {
    format!("{x:.2}")
}

fn fmt3(x: f64) -> String {
    format!("{x:.3}")
}

/// `(actual ...)` numbers: the measured row count at the top, the plan's
/// estimate below (noida doesn't count per node).
fn actual_rows(n: &Node, depth: usize, actual: Option<&Actual>) -> u64 {
    // A ModifyTable without RETURNING emits nothing; the scan below it
    // carries the count.
    let measured = match actual {
        Some(a) => a.rows,
        None => return n.rows,
    };
    match (depth, n.kind.as_str()) {
        (0, "ModifyTable") => 0,
        (0, _) => measured,
        _ if n.measured => measured,
        _ => n.rows,
    }
}

fn text(n: &Node, depth: usize, o: &Options, actual: Option<&Actual>, out: &mut Vec<String>) {
    let mut line = if depth == 0 {
        n.label()
    } else {
        format!("{}->  {}", " ".repeat(6 * (depth - 1) + 2), n.label())
    };
    if o.costs {
        line.push_str(&format!(
            "  (cost={}..{} rows={} width={})",
            fmt2(n.startup_cost()),
            fmt2(n.total_cost()),
            n.rows,
            n.width
        ));
    }
    if let Some(a) = actual {
        let t = a.execution_ms;
        let r = actual_rows(n, depth, actual);
        line.push_str(&if o.timing {
            format!(" (actual time={}..{} rows={r} loops=1)", fmt3(0.0), fmt3(t))
        } else {
            format!(" (actual rows={r} loops=1)")
        });
    }
    out.push(line);
    let pad = " ".repeat(if depth == 0 { 2 } else { 6 * depth + 2 });
    for (k, v) in &n.props {
        if matches!(k.as_str(), "Alias" | "Function Name" | "Subplan Name") {
            continue;
        }
        let v = match v {
            Prop::Text(s) => s.clone(),
            Prop::List(l) => l.join(", "),
        };
        out.push(format!("{pad}{k}: {v}"));
    }
    for c in &n.children {
        if c.parent_rel == Some("InitPlan") {
            let name = c.props.iter().find(|(k, _)| k == "Subplan Name");
            if let Some((_, Prop::Text(s))) = name {
                out.push(format!("{pad}{s}"));
            }
        }
        text(c, depth + 1, o, actual, out);
    }
}

fn summary_text(o: &Options, actual: Option<&Actual>, out: &mut Vec<String>) {
    if let Some(a) = actual
        && o.summary
    {
        out.push(format!("Planning Time: {} ms", fmt3(a.planning_ms)));
        out.push(format!("Execution Time: {} ms", fmt3(a.execution_ms)));
    }
}

/// A node's fields in Postgres's order, for the structured formats.
enum Field {
    Str(String),
    Num(String),
    Bool(bool),
    List(Vec<String>),
}

fn fields(n: &Node, depth: usize, o: &Options, actual: Option<&Actual>) -> Vec<(String, Field)> {
    let mut f: Vec<(String, Field)> = vec![("Node Type".into(), Field::Str(n.kind.clone()))];
    if let Some(s) = n.strategy {
        f.push(("Strategy".into(), Field::Str(s.into())));
    }
    if let Some(op) = n.operation {
        f.push(("Operation".into(), Field::Str(op.into())));
    }
    if let Some(p) = n.parent_rel {
        f.push(("Parent Relationship".into(), Field::Str(p.into())));
    }
    for (k, v) in &n.props {
        if k == "Subplan Name"
            && let Prop::Text(s) = v
        {
            f.push((k.clone(), Field::Str(s.clone())));
        }
    }
    f.push(("Parallel Aware".into(), Field::Bool(false)));
    f.push(("Async Capable".into(), Field::Bool(false)));
    if let Some(j) = &n.join_type {
        f.push(("Join Type".into(), Field::Str(j.clone())));
    }
    if let Some((rel, alias)) = &n.relation {
        f.push(("Relation Name".into(), Field::Str(rel.clone())));
        f.push(("Alias".into(), Field::Str(alias.clone())));
    }
    for (k, v) in &n.props {
        if (k == "Function Name" || k == "Alias")
            && let Prop::Text(s) = v
        {
            f.push((k.clone(), Field::Str(s.clone())));
        }
    }
    if o.costs {
        f.push(("Startup Cost".into(), Field::Num(fmt2(n.startup_cost()))));
        f.push(("Total Cost".into(), Field::Num(fmt2(n.total_cost()))));
        f.push(("Plan Rows".into(), Field::Num(n.rows.to_string())));
        f.push(("Plan Width".into(), Field::Num(n.width.to_string())));
    }
    if let Some(a) = actual {
        if o.timing {
            f.push(("Actual Startup Time".into(), Field::Num(fmt3(0.0))));
            f.push(("Actual Total Time".into(), Field::Num(fmt3(a.execution_ms))));
        }
        f.push(("Actual Rows".into(), Field::Num(actual_rows(n, depth, actual).to_string())));
        f.push(("Actual Loops".into(), Field::Num("1".into())));
    }
    for (k, v) in &n.props {
        if matches!(k.as_str(), "Function Name" | "Alias" | "Subplan Name") {
            continue;
        }
        f.push((
            k.clone(),
            match v {
                Prop::Text(s) => Field::Str(s.clone()),
                Prop::List(l) => Field::List(l.clone()),
            },
        ));
    }
    f
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_doc(root: &Node, o: &Options, actual: Option<&Actual>) -> String {
    let mut out = String::from("[\n  {\n    \"Plan\": ");
    json_node(root, 0, 2, o, actual, &mut out);
    if let Some(a) = actual {
        if o.summary {
            out.push_str(&format!(",\n    \"Planning Time\": {}", fmt3(a.planning_ms)));
        }
        out.push_str(",\n    \"Triggers\": [\n    ]");
        if o.summary {
            out.push_str(&format!(",\n    \"Execution Time\": {}", fmt3(a.execution_ms)));
        }
    }
    out.push_str("\n  }\n]");
    out
}

fn json_node(
    n: &Node,
    depth: usize,
    level: usize,
    o: &Options,
    actual: Option<&Actual>,
    out: &mut String,
) {
    let ind = "  ".repeat(level + 1);
    out.push('{');
    let fs = fields(n, depth, o, actual);
    let mut first = true;
    for (k, v) in fs {
        out.push_str(if first { "\n" } else { ",\n" });
        first = false;
        out.push_str(&format!("{ind}{}: ", json_str(&k)));
        match v {
            Field::Str(s) => out.push_str(&json_str(&s)),
            Field::Num(s) => out.push_str(&s),
            Field::Bool(b) => out.push_str(if b { "true" } else { "false" }),
            Field::List(l) => {
                out.push('[');
                out.push_str(&l.iter().map(|s| json_str(s)).collect::<Vec<_>>().join(", "));
                out.push(']');
            }
        }
    }
    if !n.children.is_empty() {
        out.push_str(&format!(",\n{ind}\"Plans\": ["));
        for (i, c) in n.children.iter().enumerate() {
            out.push_str(if i == 0 { "\n" } else { ",\n" });
            out.push_str(&"  ".repeat(level + 2));
            json_node(c, depth + 1, level + 2, o, actual, out);
        }
        out.push_str(&format!("\n{ind}]"));
    }
    out.push_str(&format!("\n{}}}", "  ".repeat(level)));
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn xml_doc(root: &Node, o: &Options, actual: Option<&Actual>) -> String {
    let mut out =
        String::from("<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n  <Query>\n");
    xml_node(root, 0, 2, o, actual, &mut out);
    if let Some(a) = actual {
        if o.summary {
            out.push_str(&format!("    <Planning-Time>{}</Planning-Time>\n", fmt3(a.planning_ms)));
        }
        out.push_str("    <Triggers>\n    </Triggers>\n");
        if o.summary {
            out.push_str(&format!(
                "    <Execution-Time>{}</Execution-Time>\n",
                fmt3(a.execution_ms)
            ));
        }
    }
    out.push_str("  </Query>\n</explain>");
    out
}

fn xml_node(
    n: &Node,
    depth: usize,
    level: usize,
    o: &Options,
    actual: Option<&Actual>,
    out: &mut String,
) {
    let ind = "  ".repeat(level);
    out.push_str(&format!("{ind}<Plan>\n"));
    for (k, v) in fields(n, depth, o, actual) {
        let tag = k.replace(' ', "-");
        let inner = "  ".repeat(level + 1);
        match v {
            Field::List(l) => {
                out.push_str(&format!("{inner}<{tag}>\n"));
                for item in l {
                    out.push_str(&format!("{inner}  <Item>{}</Item>\n", xml_escape(&item)));
                }
                out.push_str(&format!("{inner}</{tag}>\n"));
            }
            Field::Str(s) | Field::Num(s) => {
                out.push_str(&format!("{inner}<{tag}>{}</{tag}>\n", xml_escape(&s)))
            }
            Field::Bool(b) => out.push_str(&format!("{inner}<{tag}>{b}</{tag}>\n")),
        }
    }
    if !n.children.is_empty() {
        let inner = "  ".repeat(level + 1);
        out.push_str(&format!("{inner}<Plans>\n"));
        for c in &n.children {
            xml_node(c, depth + 1, level + 2, o, actual, out);
        }
        out.push_str(&format!("{inner}</Plans>\n"));
    }
    out.push_str(&format!("{ind}</Plan>\n"));
}

fn yaml_str(s: &str) -> String {
    // Postgres quotes every string scalar.
    json_str(s)
}

fn yaml_doc(root: &Node, o: &Options, actual: Option<&Actual>) -> String {
    let mut out = String::from("- Plan: \n");
    yaml_node(root, 0, 4, o, actual, &mut out);
    if let Some(a) = actual {
        if o.summary {
            out.push_str(&format!("  Planning Time: {}\n", fmt3(a.planning_ms)));
        }
        out.push_str("  Triggers: \n");
        if o.summary {
            out.push_str(&format!("  Execution Time: {}\n", fmt3(a.execution_ms)));
        }
    }
    out.trim_end_matches('\n').to_string()
}

/// `first_prefix` replaces the indentation of the first line (a list
/// item's "- ").
fn yaml_node(
    n: &Node,
    depth: usize,
    indent: usize,
    o: &Options,
    actual: Option<&Actual>,
    out: &mut String,
) {
    yaml_fields(n, depth, indent, None, o, actual, out)
}

fn yaml_fields(
    n: &Node,
    depth: usize,
    indent: usize,
    first_prefix: Option<String>,
    o: &Options,
    actual: Option<&Actual>,
    out: &mut String,
) {
    let ind = " ".repeat(indent);
    let mut first = first_prefix;
    let mut put = |out: &mut String, s: String| {
        match first.take() {
            Some(p) => out.push_str(&p),
            None => out.push_str(&ind),
        }
        out.push_str(&s);
        out.push('\n');
    };
    for (k, v) in fields(n, depth, o, actual) {
        match v {
            Field::Str(s) => put(out, format!("{k}: {}", yaml_str(&s))),
            Field::Num(s) => put(out, format!("{k}: {s}")),
            Field::Bool(b) => put(out, format!("{k}: {b}")),
            Field::List(l) => {
                put(out, format!("{k}: "));
                for item in l {
                    out.push_str(&format!("{ind}  - {}\n", yaml_str(&item)));
                }
            }
        }
    }
    if !n.children.is_empty() {
        put(out, "Plans: ".into());
        for c in &n.children {
            yaml_fields(c, depth + 1, indent + 4, Some(format!("{ind}  - ")), o, actual, out);
        }
    }
}

/// Reads `EXPLAIN (FORMAT x, ANALYZE, COSTS off, ...)`.
pub fn options(
    analyze: bool,
    verbose: bool,
    format: Option<&a::AnalyzeFormatKind>,
    opts: Option<&[a::UtilityOption]>,
) -> Result<Options, String> {
    let mut o = Options {
        format: Format::Text,
        analyze,
        costs: true,
        verbose,
        summary: false,
        timing: true,
    };
    if let Some(f) = format {
        o.format = parse_format(&f.to_string())?;
    }
    let mut summary = None;
    for opt in opts.unwrap_or_default() {
        let name = opt.name.value.to_lowercase();
        let arg = opt.arg.as_ref().map(|e| e.to_string().trim_matches('\'').to_lowercase());
        let on = || -> Result<bool, String> {
            match arg.as_deref() {
                None | Some("true") | Some("on") | Some("1") => Ok(true),
                Some("false") | Some("off") | Some("0") => Ok(false),
                Some(other) => Err(format!("{name} requires a Boolean value, got {other}")),
            }
        };
        match name.as_str() {
            "format" => o.format = parse_format(arg.as_deref().unwrap_or(""))?,
            "analyze" => o.analyze = on()?,
            "verbose" => o.verbose = on()?,
            "costs" => o.costs = on()?,
            "summary" => summary = Some(on()?),
            "timing" => o.timing = on()?,
            "buffers" | "wal" | "settings" | "generic_plan" | "memory" | "serialize" => {
                on()?;
            }
            _ => return Err(format!("unrecognized EXPLAIN option \"{name}\"")),
        }
    }
    o.summary = summary.unwrap_or(o.analyze);
    Ok(o)
}

fn parse_format(s: &str) -> Result<Format, String> {
    Ok(match s.trim().to_lowercase().as_str() {
        "text" => Format::Text,
        "json" => Format::Json,
        "xml" => Format::Xml,
        "yaml" => Format::Yaml,
        other => {
            return Err(format!("unrecognized value for EXPLAIN option \"format\": \"{other}\""));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn explain(sql: &str, format: Format, analyze: bool) -> Vec<String> {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap().remove(0);
        let rows = |t: &str| if t == "big" { 1000 } else { 10 };
        let root = plan(&stmt, &rows);
        let o = Options {
            format,
            analyze,
            costs: true,
            verbose: false,
            summary: analyze,
            timing: true,
        };
        let actual = analyze.then_some(Actual { rows: 3, planning_ms: 0.1, execution_ms: 0.2 });
        render(&root, &o, actual.as_ref())
    }

    #[test]
    fn text_tree() {
        let out = explain(
            "SELECT b.x, count(*) FROM big b LEFT JOIN small s ON s.id = b.sid WHERE b.x > 1 GROUP BY b.x ORDER BY 2 LIMIT 5",
            Format::Text,
            false,
        );
        let labels: Vec<&str> = out
            .iter()
            .map(|l| l.trim_start().trim_start_matches("->  ").split("  (").next().unwrap())
            .collect();
        assert_eq!(
            labels,
            [
                "Limit",
                "Sort",
                "Sort Key: 2",
                "HashAggregate",
                "Group Key: b.x",
                "Hash Left Join",
                "Hash Cond: (s.id = b.sid)",
                "Join Filter: (b.x > 1)",
                "Seq Scan on big b",
                "Hash",
                "Seq Scan on small s",
            ]
        );
        // Children are indented under "->  " the way Postgres prints them.
        assert!(out[1].starts_with("  ->  Sort"));
        assert!(out[2].starts_with("        Sort Key:"));
        assert!(out[8].starts_with("                    ->  Seq Scan on big b"));
    }

    #[test]
    fn modify_table_analyze() {
        let out = explain("UPDATE big SET x = 1 WHERE id < 5", Format::Text, true);
        assert!(out[0].starts_with("Update on big"), "{out:?}");
        assert!(out[0].contains("rows=0 loops=1"), "{out:?}");
        assert!(out[1].contains("->  Seq Scan on big") && out[1].contains("rows=3 loops=1"));
        assert_eq!(out[out.len() - 2], "Planning Time: 0.100 ms");
        assert_eq!(out[out.len() - 1], "Execution Time: 0.200 ms");
    }

    #[test]
    fn structured_formats() {
        let q = "(SELECT id FROM big) UNION (SELECT id FROM small WHERE name = 'a<b')";
        let json = explain(q, Format::Json, true).remove(0);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v[0]["Plan"]["Node Type"], "Aggregate");
        assert_eq!(v[0]["Plan"]["Plans"][0]["Node Type"], "Append");
        assert_eq!(v[0]["Plan"]["Plans"][0]["Plans"][1]["Filter"], "(name = 'a<b')");
        assert!(v[0]["Execution Time"].is_number());

        let xml = explain(q, Format::Xml, false).remove(0);
        assert!(xml.starts_with("<explain xmlns=\"http://www.postgresql.org/2009/explain\">"));
        assert!(xml.contains("<Filter>(name = 'a&lt;b')</Filter>"));
        assert_eq!(xml.matches("<Plan>").count(), xml.matches("</Plan>").count());

        let yaml = explain(q, Format::Yaml, false).remove(0);
        assert!(yaml.starts_with("- Plan: \n    Node Type: \"Aggregate\"\n"), "{yaml}");
        assert!(yaml.contains("\n    Plans: \n      - Node Type: \"Append\"\n"), "{yaml}");
    }

    #[test]
    fn options_parse() {
        let opt = |name: &str, arg: Option<&str>| a::UtilityOption {
            name: a::Ident::new(name),
            arg: arg.map(|v| a::Expr::Identifier(a::Ident::new(v))),
        };
        let o = options(
            false,
            false,
            None,
            Some(&[opt("format", Some("yaml")), opt("costs", Some("off")), opt("analyze", None)]),
        )
        .unwrap();
        assert_eq!(o.format, Format::Yaml);
        assert!(!o.costs && o.analyze && o.summary);
        assert!(options(false, false, None, Some(&[opt("format", Some("csv"))])).is_err());
        assert!(options(false, false, None, Some(&[opt("nope", None)])).is_err());
    }
}
