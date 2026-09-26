//! Sessions, transactions and statement dispatch.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use sqlparser::ast as a;

use super::binder::{Binder, SessionInfo, name_parts, unsupported};
use super::catalog::{DATABASE_OID, DbState, Row, SeqValue};
use super::ddl::Ddl;
use super::error::{PgError, PgResult, code};
use super::exec::{self, Ctx, Runtime};
use super::plan::{OutCol, Planned, Query};
use super::session::Settings;
use super::types::{self, RegNames, Type, Value};

/// A statement's result.
pub struct StmtResult {
    pub cols: Vec<OutCol>,
    pub rows: Vec<Row>,
    pub tag: String,
    pub notices: Vec<PgError>,
    /// ParameterStatus updates the client must be told about.
    pub params_changed: Vec<(String, String)>,
    /// True when the statement returns a row set (RowDescription).
    pub returns_rows: bool,
}

impl StmtResult {
    fn tag(tag: impl Into<String>) -> StmtResult {
        StmtResult {
            cols: vec![],
            rows: vec![],
            tag: tag.into(),
            notices: vec![],
            params_changed: vec![],
            returns_rows: false,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TxStatus {
    Idle,
    InTransaction,
    Failed,
}

impl TxStatus {
    pub fn byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }
}

struct Txn {
    state: DbState,
    savepoints: Vec<(String, DbState)>,
    wrote: bool,
}

#[derive(Clone)]
pub struct Prepared {
    pub sql: String,
    pub stmt: Option<a::Statement>,
    pub param_types: Vec<Type>,
    pub cols: Vec<OutCol>,
    pub returns_rows: bool,
}

pub struct Portal {
    pub statement: String,
    pub params: Vec<Value>,
    pub result_formats: Vec<i16>,
    pub cols: Vec<OutCol>,
    pub rows: Vec<Row>,
    pub pos: usize,
    pub tag: String,
    pub executed: bool,
    pub returns_rows: bool,
    pub suspended: bool,
}

pub struct Session {
    pub id: u32,
    pub pid: i32,
    pub secret: i32,
    pub rt: Runtime,
    pub status: TxStatus,
    txn: Option<Txn>,
    pub prepared: BTreeMap<String, Prepared>,
    pub portals: BTreeMap<String, Portal>,
    pub cancel: Arc<AtomicBool>,
    pub notifications: Arc<Mutex<Vec<(i32, String, String)>>>,
    /// Statements run so far in an implicit multi-statement simple query.
    pub in_implicit_tx: bool,
}

struct Global {
    db: DbState,
    seqs: BTreeMap<u32, SeqValue>,
    /// Session id currently holding the write lock.
    writer: Option<u32>,
    sessions: BTreeMap<u32, SessionHandle>,
    next_id: u32,
}

#[derive(Clone)]
struct SessionHandle {
    pid: i32,
    secret: i32,
    cancel: Arc<AtomicBool>,
    channels: Vec<String>,
    notifications: Arc<Mutex<Vec<(i32, String, String)>>>,
}

#[derive(Clone)]
pub struct Engine {
    global: Arc<Mutex<Global>>,
    next_pid: Arc<AtomicI32>,
}

impl Default for Engine {
    fn default() -> Self {
        Engine::new()
    }
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            global: Arc::new(Mutex::new(Global {
                db: DbState::default(),
                seqs: BTreeMap::new(),
                writer: None,
                sessions: BTreeMap::new(),
                next_id: 1,
            })),
            next_pid: Arc::new(AtomicI32::new(10_000)),
        }
    }

    pub fn connect(&self, user: &str, database: &str) -> Session {
        let mut g = self.global.lock().unwrap();
        let id = g.next_id;
        g.next_id += 1;
        let pid = self.next_pid.fetch_add(1, AtomicOrdering::SeqCst);
        let secret = (super::funcs::random_u64() as i32) | 1;
        let cancel = Arc::new(AtomicBool::new(false));
        let notifications = Arc::new(Mutex::new(vec![]));
        g.sessions.insert(
            id,
            SessionHandle {
                pid,
                secret,
                cancel: cancel.clone(),
                channels: vec![],
                notifications: notifications.clone(),
            },
        );
        let now = super::datetime::now_micros();
        Session {
            id,
            pid,
            secret,
            rt: Runtime {
                pid,
                user: user.to_string(),
                database: database.to_string(),
                settings: Settings::default(),
                currval: BTreeMap::new(),
                lastval: None,
                now,
                stmt_now: now,
                listening: vec![],
                notices: vec![],
            },
            status: TxStatus::Idle,
            txn: None,
            prepared: BTreeMap::new(),
            portals: BTreeMap::new(),
            cancel,
            notifications,
            in_implicit_tx: false,
        }
    }

    pub fn disconnect(&self, s: &Session) {
        let mut g = self.global.lock().unwrap();
        if g.writer == Some(s.id) {
            g.writer = None;
        }
        g.sessions.remove(&s.id);
    }

    /// Handles a CancelRequest: flags the matching session.
    pub fn cancel(&self, pid: i32, secret: i32) {
        let g = self.global.lock().unwrap();
        if let Some(h) = g.sessions.values().find(|h| h.pid == pid && h.secret == secret) {
            h.cancel.store(true, AtomicOrdering::SeqCst);
        }
    }

    /// Parses SQL into statements, mapping parser errors to 42601.
    pub fn parse_sql(&self, sql: &str) -> PgResult<Vec<a::Statement>> {
        super::parse_sql(sql)
    }

    /// Plans a statement, returning its parameter and result types.
    pub fn prepare(&self, s: &mut Session, sql: &str, param_hints: &[Type]) -> PgResult<Prepared> {
        let stmts = self.parse_sql(sql)?;
        if stmts.len() > 1 {
            return Err(PgError::new(
                code::SYNTAX_ERROR,
                "cannot insert multiple commands into a prepared statement",
            ));
        }
        let Some(stmt) = stmts.into_iter().next() else {
            return Ok(Prepared {
                sql: sql.to_string(),
                stmt: None,
                param_types: vec![],
                cols: vec![],
                returns_rows: false,
            });
        };
        let g = self.global.lock().unwrap();
        let db = s.txn.as_ref().map(|t| &t.state).unwrap_or(&g.db);
        let info = self.info(s);
        let mut b = Binder::new(db, &info, param_hints);
        let (cols, returns_rows, params) = match &stmt {
            a::Statement::Query(_)
            | a::Statement::Insert(_)
            | a::Statement::Update(_)
            | a::Statement::Delete(_) => {
                let planned = b.bind_statement(&stmt)?;
                (planned.cols, planned.returns_rows, b.params.clone())
            }
            other => {
                (describe_other(other, db, &info)?, statement_returns_rows(other), b.params.clone())
            }
        };
        let param_types =
            params.iter().map(|t| if t.is_unknown() { Type::TEXT } else { *t }).collect();
        Ok(Prepared { sql: sql.to_string(), stmt: Some(stmt), param_types, cols, returns_rows })
    }

    fn info(&self, s: &Session) -> SessionInfo {
        SessionInfo {
            user: s.rt.user.clone(),
            database: s.rt.database.clone(),
            search_path: s.rt.settings.search_path(&s.rt.user),
            fmt: s.rt.settings.fmt(),
            now: s.rt.now,
        }
    }

    /// Runs one statement, managing the transaction around it.
    pub fn execute(
        &self,
        s: &mut Session,
        stmt: &a::Statement,
        params: &[Value],
    ) -> PgResult<StmtResult> {
        if s.status == TxStatus::Failed && !is_transaction_control(stmt) {
            return Err(PgError::new(
                code::IN_FAILED_SQL_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        s.rt.stmt_now = super::datetime::now_micros();
        if s.txn.is_none() {
            s.rt.now = s.rt.stmt_now;
        }
        let result = self.run_statement(s, stmt, params);
        match &result {
            Err(e) if e.severity != "NOTICE" => {
                if s.status == TxStatus::InTransaction {
                    s.status = TxStatus::Failed;
                } else if s.txn.is_some() {
                    // Implicit transaction: roll it back.
                    self.rollback(s);
                }
            }
            _ => {}
        }
        result
    }

    fn run_statement(
        &self,
        s: &mut Session,
        stmt: &a::Statement,
        params: &[Value],
    ) -> PgResult<StmtResult> {
        use a::Statement as S;
        match stmt {
            S::StartTransaction { .. } => {
                if s.txn.is_some() && s.status != TxStatus::Idle {
                    let mut r = StmtResult::tag("BEGIN");
                    r.notices.push(warning("there is already a transaction in progress"));
                    return Ok(r);
                }
                self.begin(s);
                s.status = TxStatus::InTransaction;
                Ok(StmtResult::tag("BEGIN"))
            }
            S::Commit { .. } => {
                let tag = "COMMIT";
                if s.status == TxStatus::Idle && s.txn.is_none() {
                    let mut r = StmtResult::tag(tag);
                    r.notices.push(warning("there is no transaction in progress"));
                    return Ok(r);
                }
                if s.status == TxStatus::Failed {
                    self.rollback(s);
                    s.status = TxStatus::Idle;
                    return Ok(StmtResult::tag("ROLLBACK"));
                }
                self.commit(s)?;
                s.status = TxStatus::Idle;
                Ok(StmtResult::tag(tag))
            }
            S::Rollback { savepoint, .. } => {
                if let Some(name) = savepoint {
                    let n = name_parts(&a::ObjectName::from(vec![name.clone()]))
                        .pop()
                        .unwrap_or_default();
                    return self.rollback_to(s, &n);
                }
                if s.status == TxStatus::Idle && s.txn.is_none() {
                    let mut r = StmtResult::tag("ROLLBACK");
                    r.notices.push(warning("there is no transaction in progress"));
                    return Ok(r);
                }
                self.rollback(s);
                s.status = TxStatus::Idle;
                Ok(StmtResult::tag("ROLLBACK"))
            }
            S::Savepoint { name } => {
                let n =
                    name_parts(&a::ObjectName::from(vec![name.clone()])).pop().unwrap_or_default();
                let Some(tx) = s.txn.as_mut() else {
                    return Err(PgError::new(
                        code::NO_ACTIVE_SQL_TRANSACTION,
                        "SAVEPOINT can only be used in transaction blocks",
                    ));
                };
                let snapshot = tx.state.clone();
                tx.savepoints.push((n, snapshot));
                Ok(StmtResult::tag("SAVEPOINT"))
            }
            S::ReleaseSavepoint { name } => {
                let n =
                    name_parts(&a::ObjectName::from(vec![name.clone()])).pop().unwrap_or_default();
                let Some(tx) = s.txn.as_mut() else {
                    return Err(PgError::new(
                        code::NO_ACTIVE_SQL_TRANSACTION,
                        "RELEASE SAVEPOINT can only be used in transaction blocks",
                    ));
                };
                match tx.savepoints.iter().rposition(|(sn, _)| *sn == n) {
                    Some(i) => {
                        tx.savepoints.truncate(i);
                        Ok(StmtResult::tag("RELEASE"))
                    }
                    None => Err(PgError::new(
                        code::S_E_INVALID_SPECIFICATION,
                        format!("savepoint \"{n}\" does not exist"),
                    )),
                }
            }
            S::Set(set) => self.run_set(s, set),
            S::Reset { .. } => self.run_reset(s, stmt),
            S::ShowVariable { variable } => {
                let name = show_variable_name(variable);
                if name.eq_ignore_ascii_case("all") {
                    let mut rows = vec![];
                    for (k, v) in s.rt.settings.all() {
                        rows.push(vec![Value::text(k), Value::text(v), Value::text("")]);
                    }
                    return Ok(StmtResult {
                        cols: vec![
                            OutCol::new("name", Type::TEXT),
                            OutCol::new("setting", Type::TEXT),
                            OutCol::new("description", Type::TEXT),
                        ],
                        rows,
                        tag: "SHOW".into(),
                        notices: vec![],
                        params_changed: vec![],
                        returns_rows: true,
                    });
                }
                let value = s.rt.settings.get(&name)?;
                Ok(StmtResult {
                    cols: vec![OutCol::new(name.to_lowercase(), Type::TEXT)],
                    rows: vec![vec![Value::text(value)]],
                    tag: "SHOW".into(),
                    notices: vec![],
                    params_changed: vec![],
                    returns_rows: true,
                })
            }
            S::Discard { object_type } => {
                match object_type {
                    a::DiscardObject::ALL | a::DiscardObject::PLANS => {
                        s.prepared.clear();
                        s.portals.clear();
                    }
                    _ => {}
                }
                if matches!(object_type, a::DiscardObject::ALL) {
                    s.rt.settings = Settings::default();
                }
                Ok(StmtResult::tag(format!("DISCARD {}", format!("{object_type}").to_uppercase())))
            }
            S::LISTEN { channel } => {
                let c = channel.value.clone();
                if !s.rt.listening.contains(&c) {
                    s.rt.listening.push(c.clone());
                }
                let mut g = self.global.lock().unwrap();
                if let Some(h) = g.sessions.get_mut(&s.id)
                    && !h.channels.contains(&c)
                {
                    h.channels.push(c);
                }
                Ok(StmtResult::tag("LISTEN"))
            }
            S::UNLISTEN { channel } => {
                let c = format!("{channel}");
                let mut g = self.global.lock().unwrap();
                if let Some(h) = g.sessions.get_mut(&s.id) {
                    if c == "*" {
                        h.channels.clear();
                    } else {
                        h.channels.retain(|x| *x != c);
                    }
                }
                s.rt.listening.retain(|x| c != "*" && *x != c);
                Ok(StmtResult::tag("UNLISTEN"))
            }
            S::NOTIFY { channel, payload } => {
                let c = channel.value.clone();
                let p = payload.clone().unwrap_or_default();
                self.deliver_notify(s.pid, &c, &p);
                Ok(StmtResult::tag("NOTIFY"))
            }
            S::Prepare { name, data_types, statement } => {
                let n = super::binder::name_parts(&a::ObjectName::from(vec![name.clone()]))
                    .pop()
                    .unwrap_or_default();
                let mut hints = vec![];
                {
                    let g = self.global.lock().unwrap();
                    let db = s.txn.as_ref().map(|t| &t.state).unwrap_or(&g.db);
                    let info = self.info(s);
                    let b = Binder::new(db, &info, &[]);
                    for dt in data_types {
                        hints.push(b.data_type(dt)?.0);
                    }
                }
                let prep = self.prepare(s, &statement.to_string(), &hints)?;
                s.prepared.insert(n, prep);
                Ok(StmtResult::tag("PREPARE"))
            }
            S::Execute { name, parameters, .. } => {
                let Some(name) = name else { return Err(unsupported("EXECUTE without a name")) };
                let n = super::binder::name_parts(name).pop().unwrap_or_default();
                let prep = s.prepared.get(&n).cloned().ok_or_else(|| {
                    PgError::new(
                        code::UNDEFINED_PSTATEMENT,
                        format!("prepared statement \"{n}\" does not exist"),
                    )
                })?;
                let mut vals = vec![];
                {
                    let g = self.global.lock().unwrap();
                    let db = s.txn.as_ref().map(|t| &t.state).unwrap_or(&g.db);
                    let info = self.info(s);
                    let mut b = Binder::new(db, &info, &prep.param_types);
                    for (i, p) in parameters.iter().enumerate() {
                        let te = b.bind_expr(p)?;
                        let ty = prep.param_types.get(i).copied().unwrap_or(Type::TEXT);
                        let e =
                            b.coerce(te, ty, -1, super::casts::CastCtx::Assignment, "EXECUTE")?;
                        match e {
                            super::plan::Expr::Const(v) => vals.push(v),
                            _ => return Err(unsupported("non-constant EXECUTE parameter")),
                        }
                    }
                }
                let Some(inner) = prep.stmt.clone() else { return Ok(StmtResult::tag("EXECUTE")) };
                self.run_statement(s, &inner, &vals)
            }
            S::Deallocate { name, .. } => {
                if name.value.eq_ignore_ascii_case("all") {
                    s.prepared.clear();
                } else {
                    s.prepared.remove(&name.value.to_lowercase());
                }
                Ok(StmtResult::tag("DEALLOCATE"))
            }
            S::Explain { statement, analyze, .. } => {
                if *analyze {
                    return Err(unsupported("EXPLAIN ANALYZE"));
                }
                let prepared = self.prepare(s, &statement.to_string(), &[])?;
                let _ = prepared;
                let line = format!("{} (cost=0.00..0.00 rows=0 width=0)", explain_node(statement));
                Ok(StmtResult {
                    cols: vec![OutCol::new("QUERY PLAN", Type::TEXT)],
                    rows: vec![vec![Value::text(line)]],
                    tag: "EXPLAIN".into(),
                    notices: vec![],
                    params_changed: vec![],
                    returns_rows: true,
                })
            }
            S::Analyze { .. } => Ok(StmtResult::tag("ANALYZE")),
            S::Vacuum { .. } => Ok(StmtResult::tag("VACUUM")),
            S::Grant { .. } => Ok(StmtResult::tag("GRANT")),
            S::Revoke { .. } => Ok(StmtResult::tag("REVOKE")),
            S::Lock { .. } => Ok(StmtResult::tag("LOCK TABLE")),
            S::CreateRole { .. } => Ok(StmtResult::tag("CREATE ROLE")),
            S::CreateExtension { .. } => Ok(StmtResult::tag("CREATE EXTENSION")),
            S::DropExtension { .. } => Ok(StmtResult::tag("DROP EXTENSION")),
            other => self.run_data_statement(s, other, params),
        }
    }

    /// Everything that touches the database: queries, DML and DDL.
    fn run_data_statement(
        &self,
        s: &mut Session,
        stmt: &a::Statement,
        params: &[Value],
    ) -> PgResult<StmtResult> {
        let writes = statement_writes(stmt);
        let implicit = s.txn.is_none();
        if implicit {
            self.begin(s);
        }
        if writes {
            self.acquire_writer(s)?;
        }
        let out = self.run_in_txn(s, stmt, params);
        match (&out, implicit) {
            (Ok(_), true) => self.commit(s)?,
            (Err(_), true) => self.rollback(s),
            _ => {}
        }
        out
    }

    fn run_in_txn(
        &self,
        s: &mut Session,
        stmt: &a::Statement,
        params: &[Value],
    ) -> PgResult<StmtResult> {
        let mut g = self.global.lock().unwrap();
        let global = &mut *g;
        let txn = s.txn.as_mut().expect("transaction");
        let mut ctx = Ctx {
            db: &mut txn.state,
            seqs: &mut global.seqs,
            rt: &mut s.rt,
            params,
            outer: vec![],
            ctes: vec![],
            notifies: vec![],
            affected: 0,
        };
        let info = SessionInfo {
            user: ctx.rt.user.clone(),
            database: ctx.rt.database.clone(),
            search_path: ctx.rt.settings.search_path(&ctx.rt.user),
            fmt: ctx.rt.settings.fmt(),
            now: ctx.rt.now,
        };
        let result = run_one(&mut ctx, stmt, &info);
        let notifies = std::mem::take(&mut ctx.notifies);
        drop(g);
        for (chan, payload) in notifies {
            self.deliver_notify(s.pid, &chan, &payload);
        }
        result
    }

    fn deliver_notify(&self, from_pid: i32, channel: &str, payload: &str) {
        let g = self.global.lock().unwrap();
        for h in g.sessions.values() {
            if h.channels.iter().any(|c| c == channel) {
                h.notifications.lock().unwrap().push((
                    from_pid,
                    channel.to_string(),
                    payload.to_string(),
                ));
            }
        }
    }

    fn run_set(&self, s: &mut Session, set: &a::Set) -> PgResult<StmtResult> {
        let mut changed = vec![];
        match set {
            a::Set::SingleAssignment { scope, variable, values, .. } => {
                if matches!(scope, Some(a::ContextModifier::Local)) && s.txn.is_none() {
                    // SET LOCAL outside a transaction is a no-op with a warning.
                }
                let name = name_parts(variable).join(".");
                let value = set_value_text(values)?;
                if let Some(c) = s.rt.settings.set(&name, &value)? {
                    changed.push((c.to_string(), s.rt.settings.get(c)?));
                }
            }
            a::Set::SetTimeZone { value, .. } => {
                let v = match value {
                    a::Expr::Value(v) => match &v.value {
                        a::Value::SingleQuotedString(s) => s.clone(),
                        other => other.to_string(),
                    },
                    a::Expr::Identifier(id) => id.value.clone(),
                    a::Expr::TypedString(ts) => match &ts.value.value {
                        a::Value::SingleQuotedString(s) => s.clone(),
                        o => o.to_string(),
                    },
                    other => other.to_string(),
                };
                let v = if v.eq_ignore_ascii_case("default") || v.eq_ignore_ascii_case("local") {
                    "UTC".into()
                } else {
                    v
                };
                if let Some(c) = s.rt.settings.set("TimeZone", &v)? {
                    changed.push((c.to_string(), s.rt.settings.get(c)?));
                }
            }
            a::Set::SetNames { charset_name, .. } => {
                let v = charset_name.value.clone();
                if let Some(c) = s.rt.settings.set("client_encoding", &v)? {
                    changed.push((c.to_string(), s.rt.settings.get(c)?));
                }
            }
            a::Set::SetNamesDefault {} => {}
            a::Set::SetTransaction { modes, .. } => {
                for m in modes {
                    match m {
                        a::TransactionMode::AccessMode(a::TransactionAccessMode::ReadOnly) => {
                            s.rt.settings.set("transaction_read_only", "on")?;
                        }
                        a::TransactionMode::AccessMode(a::TransactionAccessMode::ReadWrite) => {
                            s.rt.settings.set("transaction_read_only", "off")?;
                        }
                        a::TransactionMode::IsolationLevel(l) => {
                            let v = format!("{l}").to_lowercase();
                            s.rt.settings.set("transaction_isolation", &v)?;
                        }
                    }
                }
                return Ok(StmtResult::tag("SET"));
            }
            a::Set::SetSessionParam(p) => {
                return Err(unsupported(&format!("SET {p}")));
            }
            a::Set::SetRole { role_name, .. } => {
                let name = role_name
                    .as_ref()
                    .map(|r| r.value.clone())
                    .unwrap_or_else(|| s.rt.user.clone());
                if let Some(c) = s.rt.settings.set("session_authorization", &name)? {
                    changed.push((c.to_string(), s.rt.settings.get(c)?));
                }
            }
            other => return Err(unsupported(&format!("SET {other}"))),
        }
        let mut r = StmtResult::tag("SET");
        r.params_changed = changed;
        Ok(r)
    }

    fn run_reset(&self, s: &mut Session, stmt: &a::Statement) -> PgResult<StmtResult> {
        let a::Statement::Reset(r) = stmt else { return Ok(StmtResult::tag("RESET")) };
        let n = match &r.reset {
            a::Reset::ALL => "all".to_string(),
            a::Reset::SessionAuthorization => "session_authorization".to_string(),
            a::Reset::ConfigurationParameter(n) => name_parts(n).join("."),
        };
        let mut changed = vec![];
        if let Some(c) = s.rt.settings.reset(&n)? {
            changed.push((c.to_string(), s.rt.settings.get(c)?));
        }
        if n.eq_ignore_ascii_case("all") {
            for c in Settings::reported() {
                changed.push((c.to_string(), s.rt.settings.get(c)?));
            }
        }
        let mut r = StmtResult::tag("RESET");
        r.params_changed = changed;
        Ok(r)
    }

    /// Starts a transaction that spans several protocol messages.
    pub fn begin_implicit(&self, s: &mut Session) {
        if s.txn.is_none() {
            self.begin(s);
            s.in_implicit_tx = true;
        }
    }

    pub fn commit_implicit(&self, s: &mut Session) -> PgResult<()> {
        if s.in_implicit_tx {
            s.in_implicit_tx = false;
            if s.status != TxStatus::InTransaction {
                return self.commit(s);
            }
        }
        Ok(())
    }

    pub fn rollback_implicit(&self, s: &mut Session) {
        if s.in_implicit_tx {
            s.in_implicit_tx = false;
            if s.status != TxStatus::InTransaction {
                self.rollback(s);
                s.status = TxStatus::Idle;
            }
        }
    }

    // -----------------------------------------------------------------
    // Transactions

    fn begin(&self, s: &mut Session) {
        let g = self.global.lock().unwrap();
        s.rt.now = super::datetime::now_micros();
        s.txn = Some(Txn { state: g.db.clone(), savepoints: vec![], wrote: false });
    }

    /// Takes the single write lock, waiting for another transaction to finish.
    fn acquire_writer(&self, s: &mut Session) -> PgResult<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            {
                let mut g = self.global.lock().unwrap();
                match g.writer {
                    Some(id) if id != s.id => {}
                    _ => {
                        g.writer = Some(s.id);
                        if let Some(tx) = s.txn.as_mut() {
                            if !tx.wrote {
                                // Read committed: start writing from the latest state.
                                tx.state = g.db.clone();
                            }
                            tx.wrote = true;
                        }
                        return Ok(());
                    }
                }
            }
            if std::time::Instant::now() > deadline {
                return Err(PgError::new(
                    code::LOCK_NOT_AVAILABLE,
                    "could not obtain lock on database: another transaction is writing",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn commit(&self, s: &mut Session) -> PgResult<()> {
        let Some(tx) = s.txn.take() else { return Ok(()) };
        let mut g = self.global.lock().unwrap();
        if tx.wrote {
            g.db = tx.state;
        }
        if g.writer == Some(s.id) {
            g.writer = None;
        }
        Ok(())
    }

    fn rollback(&self, s: &mut Session) {
        s.txn = None;
        let mut g = self.global.lock().unwrap();
        if g.writer == Some(s.id) {
            g.writer = None;
        }
    }

    fn rollback_to(&self, s: &mut Session, name: &str) -> PgResult<StmtResult> {
        let Some(tx) = s.txn.as_mut() else {
            return Err(PgError::new(
                code::NO_ACTIVE_SQL_TRANSACTION,
                "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
            ));
        };
        match tx.savepoints.iter().rposition(|(sn, _)| sn == name) {
            Some(i) => {
                tx.state = tx.savepoints[i].1.clone();
                tx.savepoints.truncate(i + 1);
                s.status = TxStatus::InTransaction;
                Ok(StmtResult::tag("ROLLBACK"))
            }
            None => Err(PgError::new(
                code::S_E_INVALID_SPECIFICATION,
                format!("savepoint \"{name}\" does not exist"),
            )),
        }
    }

    /// A snapshot of the database for read-only inspection (Describe).
    pub fn with_db<T>(&self, s: &Session, f: impl FnOnce(&DbState) -> T) -> T {
        match &s.txn {
            Some(tx) => f(&tx.state),
            None => {
                let g = self.global.lock().unwrap();
                f(&g.db)
            }
        }
    }

    /// Names for reg* values in a result set.
    pub fn reg_names(&self, s: &Session, cols: &[OutCol]) -> Option<Arc<RegNames>> {
        if !cols.iter().any(|c| c.ty.is_reg()) {
            return None;
        }
        Some(Arc::new(self.with_db(s, |db| {
            let mut r = RegNames::default();
            for t in db.tables.values() {
                r.class.insert(t.oid, t.name.clone());
                for i in &t.indexes {
                    r.class.insert(i.oid, i.name.clone());
                }
            }
            for sq in db.sequences.values() {
                r.class.insert(sq.oid, sq.name.clone());
            }
            for ti in types::TYPES {
                r.types.insert(ti.oid, Type::of(ti.base).display(-1));
                if ti.array_oid != 0 {
                    r.types.insert(ti.array_oid, Type::array_of(ti.base).display(-1));
                }
            }
            for e in db.enums.values() {
                r.types.insert(e.oid, e.name.clone());
            }
            for sig in super::sigs::all_sigs() {
                r.procs.insert(sig.oid, sig.name.to_string());
            }
            for sc in db.schemas.values() {
                r.namespaces.insert(sc.oid, sc.name.clone());
            }
            r.roles.insert(10, s.rt.user.clone());
            r
        })))
    }
}

fn warning(msg: &str) -> PgError {
    PgError { severity: "WARNING", ..PgError::new(code::WARNING, msg) }
}

fn is_transaction_control(stmt: &a::Statement) -> bool {
    matches!(stmt, a::Statement::Commit { .. } | a::Statement::Rollback { .. })
}

fn statement_writes(stmt: &a::Statement) -> bool {
    use a::Statement as S;
    match stmt {
        S::Query(q) => {
            // Data-modifying CTEs write.
            let sql = q.to_string().to_lowercase();
            sql.contains("insert ")
                || sql.contains("update ")
                || sql.contains("delete ")
                || sql.contains("nextval")
        }
        S::Insert(_) | S::Update(_) | S::Delete(_) => true,
        _ => true,
    }
}

fn statement_returns_rows(stmt: &a::Statement) -> bool {
    matches!(stmt, a::Statement::ShowVariable { .. } | a::Statement::Explain { .. })
}

/// `SHOW TRANSACTION ISOLATION LEVEL` and `SHOW TIME ZONE` name settings in
/// words; everything else joins the words with underscores.
fn show_variable_name(variable: &[a::Ident]) -> String {
    let phrase = variable.iter().map(|i| i.value.to_lowercase()).collect::<Vec<_>>().join(" ");
    match phrase.as_str() {
        "transaction isolation level" => "transaction_isolation".into(),
        "transaction read only" => "transaction_read_only".into(),
        "transaction deferrable" => "transaction_deferrable".into(),
        "time zone" => "TimeZone".into(),
        "session authorization" => "session_authorization".into(),
        "server version" => "server_version".into(),
        "server encoding" => "server_encoding".into(),
        "client encoding" => "client_encoding".into(),
        _ => variable.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("_"),
    }
}

fn describe_other(
    stmt: &a::Statement,
    _db: &DbState,
    _info: &SessionInfo,
) -> PgResult<Vec<OutCol>> {
    Ok(match stmt {
        a::Statement::ShowVariable { variable } => {
            let name = show_variable_name(variable);
            if name.eq_ignore_ascii_case("all") {
                vec![
                    OutCol::new("name", Type::TEXT),
                    OutCol::new("setting", Type::TEXT),
                    OutCol::new("description", Type::TEXT),
                ]
            } else {
                vec![OutCol::new(name.to_lowercase(), Type::TEXT)]
            }
        }
        a::Statement::Explain { .. } => vec![OutCol::new("QUERY PLAN", Type::TEXT)],
        _ => vec![],
    })
}

fn explain_node(stmt: &a::Statement) -> String {
    match stmt {
        a::Statement::Query(_) => "Seq Scan".into(),
        a::Statement::Insert(_) => "Insert".into(),
        a::Statement::Update(_) => "Update".into(),
        a::Statement::Delete(_) => "Delete".into(),
        _ => "Result".into(),
    }
}

fn set_value_text(values: &[a::Expr]) -> PgResult<String> {
    let mut parts = vec![];
    for v in values {
        parts.push(match v {
            a::Expr::Value(x) => match &x.value {
                a::Value::SingleQuotedString(s) => s.clone(),
                a::Value::Number(n, _) => n.to_string(),
                a::Value::Boolean(b) => b.to_string(),
                other => other.to_string(),
            },
            a::Expr::Identifier(id) => id.value.clone(),
            a::Expr::CompoundIdentifier(ids) => {
                ids.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
            }
            other => other.to_string(),
        });
    }
    Ok(parts.join(", "))
}

/// Runs a query, DML or DDL statement inside an open transaction.
fn run_one(ctx: &mut Ctx, stmt: &a::Statement, info: &SessionInfo) -> PgResult<StmtResult> {
    use a::Statement as S;
    match stmt {
        S::Query(_) | S::Insert(_) | S::Update(_) | S::Delete(_) => {
            let db = ctx.db.clone();
            let mut b = Binder::new(&db, info, &param_types(ctx));
            let planned = b.bind_statement(stmt)?;
            run_planned(ctx, planned, stmt)
        }
        S::CreateTable(ct) => {
            let mut d = ddl(ctx, info);
            let tag = d.create_table(ct)?;
            Ok(StmtResult::tag(tag))
        }
        S::CreateView(cv) => {
            let mut d = ddl(ctx, info);
            let tag =
                d.create_view(&cv.name, &cv.query, &cv.columns, cv.or_replace, cv.materialized)?;
            Ok(StmtResult::tag(tag))
        }
        S::CreateIndex(ci) => {
            let mut d = ddl(ctx, info);
            let tag = d.create_index(ci)?;
            Ok(StmtResult::tag(tag))
        }
        S::CreateSchema { schema_name, if_not_exists, .. } => {
            let mut d = ddl(ctx, info);
            let tag = d.create_schema(schema_name, *if_not_exists)?;
            Ok(StmtResult::tag(tag))
        }
        S::CreateSequence { name, if_not_exists, sequence_options, owned_by, .. } => {
            let cs = super::ddl::CreateSequenceStmt {
                name,
                if_not_exists: *if_not_exists,
                sequence_options,
                owned_by: owned_by.as_ref(),
            };
            let mut d = ddl(ctx, info);
            let tag = d.create_sequence(&cs)?;
            Ok(StmtResult::tag(tag))
        }
        S::CreateType { name, representation } => {
            let Some(a::UserDefinedTypeRepresentation::Enum { labels }) = representation else {
                return Err(unsupported("CREATE TYPE of this kind"));
            };
            let mut d = ddl(ctx, info);
            let tag = d.create_enum(name, labels)?;
            Ok(StmtResult::tag(tag))
        }
        S::AlterTable(at) => {
            let mut d = ddl(ctx, info);
            let tag = d.alter_table(&at.name, &at.operations, at.if_exists)?;
            Ok(StmtResult::tag(tag))
        }
        S::Drop { .. } => {
            let mut d = ddl(ctx, info);
            let tag = d.drop(stmt)?;
            Ok(StmtResult::tag(tag))
        }
        S::Truncate(t) => {
            let restart = matches!(t.identity, Some(a::TruncateIdentityOption::Restart));
            let mut d = ddl(ctx, info);
            let tag = d.truncate(&t.table_names, restart)?;
            Ok(StmtResult::tag(tag))
        }
        S::Comment { .. } => {
            let mut d = ddl(ctx, info);
            let tag = d.comment(stmt)?;
            Ok(StmtResult::tag(tag))
        }
        other => Err(unsupported(&format!("statement: {}", first_words(&other.to_string())))),
    }
}

fn ddl<'a, 'b>(ctx: &'a mut Ctx<'b>, info: &SessionInfo) -> Ddl<'a, 'b> {
    Ddl {
        ctx,
        info: SessionInfo {
            user: info.user.clone(),
            database: info.database.clone(),
            search_path: info.search_path.clone(),
            fmt: info.fmt.clone(),
            now: info.now,
        },
    }
}

fn param_types(ctx: &Ctx) -> Vec<Type> {
    ctx.params.iter().map(types::value_type_guess).collect()
}

fn run_planned(ctx: &mut Ctx, planned: Planned, stmt: &a::Statement) -> PgResult<StmtResult> {
    ctx.ctes = vec![None; planned.cte_slots];
    ctx.affected = 0;
    let rows = exec::run_query(&planned.query, ctx)?;
    let count = match &planned.query {
        Query::Dml(_) => ctx.affected,
        _ => rows.len(),
    };
    let tag = match planned.tag {
        "INSERT" => format!("INSERT 0 {count}"),
        t => format!("{t} {count}"),
    };
    let _ = stmt;
    Ok(StmtResult {
        cols: planned.cols,
        rows,
        tag,
        notices: vec![],
        params_changed: vec![],
        returns_rows: planned.returns_rows,
    })
}

fn first_words(s: &str) -> String {
    s.split_whitespace().take(4).collect::<Vec<_>>().join(" ")
}

/// Database-wide OID of the current database, for catalogs.
pub const CURRENT_DATABASE_OID: u32 = DATABASE_OID;
