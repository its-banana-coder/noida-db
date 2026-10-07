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
use super::types::{RegNames, Type, Value};

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
    /// Settings as the transaction began (ROLLBACK restores them; COMMIT
    /// restores those set LOCAL), and as each savepoint was taken.
    settings: Settings,
    savepoint_settings: Vec<Settings>,
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

/// A `DECLARE ... CURSOR FOR` cursor: the query runs eagerly right away
/// (this engine has no lazy/streaming execution), and `FETCH` just slices
/// the already-materialized rows — the same shape `Portal` already uses
/// for extended-protocol row-limited fetches.
pub struct Cursor {
    pub cols: Vec<OutCol>,
    pub rows: Vec<Row>,
    pub pos: usize,
    /// `WITH HOLD`: outlives the transaction's COMMIT.
    pub hold: bool,
    /// Declared in the current transaction (a ROLLBACK drops it).
    pub new: bool,
    /// What `pg_cursors` shows.
    pub info: super::exec::CursorInfo,
}

/// Aggregates that make a cursor's plan non-scrollable (a heuristic for
/// `pg_cursors.is_scrollable`).
const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "array_agg",
    "string_agg",
    "bool_and",
    "bool_or",
    "json_agg",
    "jsonb_agg",
];

/// A cursor's name: folded to lower case unless quoted.
fn cursor_key(name: &a::Ident) -> String {
    if name.quote_style.is_some() { name.value.clone() } else { name.value.to_lowercase() }
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
    /// `DECLARE`d cursors. Not `WITH HOLD`-aware: cleared on commit and
    /// rollback like an ordinary (non-holdable) cursor, since that's the
    /// common case and a `WITH HOLD` cursor surviving its transaction is
    /// a rare, P1-scale pattern.
    pub cursors: BTreeMap<String, Cursor>,
    pub cancel: Arc<AtomicBool>,
    pub notifications: Arc<Mutex<Vec<(i32, String, String)>>>,
    /// Statements run so far in an implicit multi-statement simple query.
    pub in_implicit_tx: bool,
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotDb {
    pub oid: u32,
    pub name: String,
    pub db: DbState,
    pub seqs: BTreeMap<u32, SeqValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub databases: BTreeMap<String, SnapshotDb>,
    pub next_db_oid: u32,
}

struct GlobalDb {
    oid: u32,
    name: String,
    db: DbState,
    seqs: BTreeMap<u32, SeqValue>,
}

struct Global {
    databases: BTreeMap<String, GlobalDb>,
    /// Session id currently holding the write lock.
    writer: Option<u32>,
    sessions: BTreeMap<u32, SessionHandle>,
    next_id: u32,
    next_db_oid: u32,
}

#[derive(Clone)]
struct SessionHandle {
    channels: Vec<String>,
    notifications: Arc<Mutex<Vec<(i32, String, String)>>>,
    database: String,
}

/// Each session's cancel flag, by (pid, secret key).
type CancelFlags = BTreeMap<(i32, i32), Arc<AtomicBool>>;

#[derive(Clone)]
pub struct Engine {
    global: Arc<Mutex<Global>>,
    next_pid: Arc<AtomicI32>,
    /// Cancel flags by (pid, secret), apart from `global`, which a running
    /// statement holds: a CancelRequest must get through meanwhile.
    cancels: Arc<Mutex<CancelFlags>>,
}

impl Default for Engine {
    fn default() -> Self {
        Engine::new()
    }
}

impl Engine {
    pub fn snapshot(&self) -> Snapshot {
        let g = self.global.lock().unwrap();
        let mut databases = BTreeMap::new();
        for (name, db) in &g.databases {
            databases.insert(
                name.clone(),
                SnapshotDb {
                    oid: db.oid,
                    name: db.name.clone(),
                    db: {
                        // Temporary objects don't outlive their sessions.
                        let mut d = db.db.clone();
                        d.drop_temp_schemas();
                        d
                    },
                    seqs: db.seqs.clone(),
                },
            );
        }
        Snapshot { databases, next_db_oid: g.next_db_oid }
    }

    pub fn new_persistent(snapshot: Snapshot) -> Engine {
        let mut databases = BTreeMap::new();
        for (name, mut db) in snapshot.databases {
            db.db.drop_temp_schemas();
            databases.insert(
                name.clone(),
                GlobalDb { oid: db.oid, name: db.name, db: db.db, seqs: db.seqs },
            );
        }
        Engine {
            global: Arc::new(Mutex::new(Global {
                databases,
                writer: None,
                sessions: BTreeMap::new(),
                next_id: 1,
                next_db_oid: snapshot.next_db_oid,
            })),
            next_pid: Arc::new(AtomicI32::new(10_000)),
            cancels: Default::default(),
        }
    }

    pub fn new() -> Engine {
        let mut databases = BTreeMap::new();
        databases.insert(
            "postgres".to_string(),
            GlobalDb {
                oid: super::catalog::DATABASE_OID,
                name: "postgres".to_string(),
                db: DbState::default(),
                seqs: BTreeMap::new(),
            },
        );
        Engine {
            global: Arc::new(Mutex::new(Global {
                databases,
                writer: None,
                sessions: BTreeMap::new(),
                next_id: 1,
                next_db_oid: super::catalog::DATABASE_OID + 1,
            })),
            next_pid: Arc::new(AtomicI32::new(10_000)),
            cancels: Default::default(),
        }
    }

    pub fn connect(&self, user: &str, database: &str) -> PgResult<Session> {
        let mut g = self.global.lock().unwrap();
        if !g.databases.contains_key(database) {
            return Err(PgError::new(
                code::INVALID_CATALOG_NAME,
                format!("database \"{database}\" does not exist"),
            ));
        }
        let id = g.next_id;
        g.next_id += 1;
        let pid = self.next_pid.fetch_add(1, AtomicOrdering::SeqCst);
        let secret = (super::funcs::random_u64() as i32) | 1;
        let cancel = Arc::new(AtomicBool::new(false));
        self.cancels.lock().unwrap().insert((pid, secret), cancel.clone());
        let notifications = Arc::new(Mutex::new(vec![]));
        g.sessions.insert(
            id,
            SessionHandle {
                channels: vec![],
                notifications: notifications.clone(),
                database: database.to_string(),
            },
        );
        let now = super::datetime::now_micros();
        let mut settings = Settings::default();
        settings.temp_schema = format!("pg_temp_{id}");
        Ok(Session {
            id,
            pid,
            secret,
            rt: Runtime {
                cursors: vec![],
                pid,
                user: user.to_string(),
                database: database.to_string(),
                settings,
                currval: BTreeMap::new(),
                lastval: None,
                now,
                stmt_now: now,
                listening: vec![],
                notices: vec![],
                cancel: cancel.clone(),
                deadline: None,
                ticks: 0,
                deferred_all: None,
                deferred: BTreeMap::new(),
                local_settings: vec![],
            },
            status: TxStatus::Idle,
            txn: None,
            prepared: BTreeMap::new(),
            portals: BTreeMap::new(),
            cursors: BTreeMap::new(),
            cancel,
            notifications,
            in_implicit_tx: false,
        })
    }

    /// Ends a session: its temporary schema is dropped (once no other
    /// transaction is writing, since a commit replaces the whole state).
    pub fn disconnect(&self, s: &Session) {
        self.cancels.lock().unwrap().remove(&(s.pid, s.secret));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let mut g = self.global.lock().unwrap();
            let free = g.writer.is_none() || g.writer == Some(s.id);
            if free && let Some(d) = g.databases.get_mut(&s.rt.database) {
                d.db.drop_schema_objects(&s.rt.settings.temp_schema);
            }
            if free || std::time::Instant::now() > deadline {
                if g.writer == Some(s.id) {
                    g.writer = None;
                }
                g.sessions.remove(&s.id);
                return;
            }
            drop(g);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    /// Handles a CancelRequest: flags the matching session.
    pub fn cancel(&self, pid: i32, secret: i32) {
        if let Some(c) = self.cancels.lock().unwrap().get(&(pid, secret)) {
            c.store(true, AtomicOrdering::SeqCst);
        }
    }

    /// Parses SQL into statements, mapping parser errors to 42601.
    pub fn parse_sql(&self, sql: &str) -> PgResult<Vec<a::Statement>> {
        super::parse_sql(sql)
    }

    /// Plans a statement, returning its parameter and result types.
    pub fn prepare(&self, s: &mut Session, sql: &str, param_hints: &[Type]) -> PgResult<Prepared> {
        super::catalog::set_session_temp_schema(&s.rt.settings.temp_schema);
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
        let db = s
            .txn
            .as_ref()
            .map(|t| &t.state)
            .unwrap_or(&g.databases.get(&s.rt.database).unwrap().db);
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
            search_path: s.rt.settings.lookup_path(&s.rt.user),
            fmt: s.rt.settings.fmt(),
            now: s.rt.now,
        }
    }

    /// Runs one statement, managing the transaction around it. `param_types`
    /// are the types `params` were already decoded with (from `Prepared`,
    /// see `prepare`) — re-binding the statement here (see `run_one`) must
    /// reuse them rather than re-deriving types from the decoded values:
    /// a client-declared parameter type is authoritative and doesn't
    /// change just because this statement uses it in a different context
    /// (e.g. an `int2`-declared parameter assigned into a `numeric`
    /// column stays `int2`, cast to `numeric` at the point of use, the
    /// same way `prepare` itself resolved it), and some values (e.g. a
    /// `json` parameter, stored as `Value::Text`) can't be told apart
    /// from a same-shaped value of a different type at all once decoded.
    pub fn execute(
        &self,
        s: &mut Session,
        stmt: &a::Statement,
        params: &[Value],
        param_types: &[Type],
    ) -> PgResult<StmtResult> {
        super::catalog::set_session_temp_schema(&s.rt.settings.temp_schema);
        s.rt.cursors = s.cursors.values().map(|c| c.info.clone()).collect();
        if s.status == TxStatus::Failed && !is_transaction_control(stmt) {
            return Err(PgError::new(
                code::IN_FAILED_SQL_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        s.rt.stmt_now = super::datetime::now_micros();
        s.rt.start_statement();
        if s.txn.is_none() {
            s.rt.now = s.rt.stmt_now;
        }
        // Reported settings (TimeZone, DateStyle, ...) the statement changed
        // -- by SET, set_config(), RESET or a transaction ending -- are sent
        // to the client as ParameterStatus, which drivers act on (psycopg
        // converts timestamptz with the reported TimeZone).
        let reported_before: Vec<(String, String)> = super::session::Settings::reported()
            .map(|n| (n.to_string(), s.rt.settings.get(n).unwrap_or_default()))
            .collect();
        let mut result = self.run_statement(s, stmt, params, param_types);
        if let Ok(r) = &mut result {
            r.params_changed = reported_before
                .into_iter()
                .filter_map(|(n, old)| {
                    let now = s.rt.settings.get(&n).unwrap_or_default();
                    (now != old).then_some((n, now))
                })
                .collect();
        }
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
        param_types: &[Type],
    ) -> PgResult<StmtResult> {
        use a::Statement as S;
        match stmt {
            S::CreateDatabase { db_name, if_not_exists, .. } => {
                let name = super::binder::name_parts(db_name).pop().unwrap_or_default();
                if s.status == TxStatus::InTransaction || s.txn.is_some() {
                    return Err(PgError::new(
                        code::ACTIVE_SQL_TRANSACTION,
                        "CREATE DATABASE cannot run inside a transaction block",
                    ));
                }
                let mut g = self.global.lock().unwrap();
                if g.databases.contains_key(&name) {
                    if !if_not_exists {
                        return Err(PgError::new(
                            code::DUPLICATE_DATABASE,
                            format!("database \"{name}\" already exists"),
                        ));
                    }
                } else {
                    let oid = g.next_db_oid;
                    g.next_db_oid += 1;
                    g.databases.insert(
                        name.clone(),
                        GlobalDb { oid, name, db: DbState::default(), seqs: BTreeMap::new() },
                    );
                }
                crate::persistence::mark("postgres");
                Ok(StmtResult::tag("CREATE DATABASE"))
            }
            S::Drop { object_type: a::ObjectType::Database, if_exists, names, .. } => {
                if s.status == TxStatus::InTransaction || s.txn.is_some() {
                    return Err(PgError::new(
                        code::ACTIVE_SQL_TRANSACTION,
                        "DROP DATABASE cannot run inside a transaction block",
                    ));
                }
                let mut g = self.global.lock().unwrap();
                let mut r = StmtResult::tag("DROP DATABASE");
                for name in names {
                    let n = super::binder::name_parts(name).pop().unwrap_or_default();
                    if g.sessions.values().any(|h| h.database == n) {
                        return Err(PgError::new(
                            code::OBJECT_IN_USE,
                            "cannot drop the currently open database".to_string(),
                        ));
                    }
                    if g.databases.remove(&n).is_none() {
                        if !if_exists {
                            return Err(PgError::new(
                                code::INVALID_CATALOG_NAME,
                                format!("database \"{n}\" does not exist"),
                            ));
                        }
                        r.notices.push(PgError::notice(format!(
                            "database \"{n}\" does not exist, skipping"
                        )));
                    }
                }
                crate::persistence::mark("postgres");
                Ok(r)
            }
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
                tx.savepoint_settings.push(s.rt.settings.clone());
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
                    cols: vec![OutCol::new(name.clone(), Type::TEXT)],
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
                    let db = s
                        .txn
                        .as_ref()
                        .map(|t| &t.state)
                        .unwrap_or(&g.databases.get(&s.rt.database).unwrap().db);
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
                    let db = s
                        .txn
                        .as_ref()
                        .map(|t| &t.state)
                        .unwrap_or(&g.databases.get(&s.rt.database).unwrap().db);
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
                self.run_statement(s, &inner, &vals, &prep.param_types)
            }
            S::Declare { stmts } => {
                for d in stmts {
                    let Some(for_query) = &d.for_query else {
                        return Err(unsupported("DECLARE without CURSOR FOR"));
                    };
                    if d.names.len() != 1 {
                        return Err(unsupported("DECLARE of multiple cursor names"));
                    }
                    let name = cursor_key(&d.names[0]);
                    let hold = d.hold == Some(true);
                    if !hold && s.status != TxStatus::InTransaction {
                        return Err(PgError::new(
                            "25P01",
                            "DECLARE CURSOR can only be used in transaction blocks",
                        ));
                    }
                    // The engine has no lazy/streaming execution, so the
                    // cursor's query just runs eagerly right now, in
                    // whatever transaction is already open (an ordinary
                    // data statement); `FETCH` below only slices the
                    // already-materialized rows.
                    let q_stmt = a::Statement::Query(for_query.clone());
                    let result = self.run_data_statement(s, &q_stmt, &[], &[])?;
                    // Without SCROLL / NO SCROLL, Postgres allows scrolling
                    // when the plan can run backwards: a plain scan, not a
                    // FROM-less SELECT or an aggregate.
                    let scroll = d.scroll.unwrap_or_else(|| match for_query.body.as_ref() {
                        a::SetExpr::Select(sel) => {
                            !sel.from.is_empty()
                                && matches!(&sel.group_by, a::GroupByExpr::Expressions(e, _) if e.is_empty())
                                && sel.having.is_none()
                                && !sel.projection.iter().any(|p| {
                                    let p = p.to_string().to_lowercase();
                                    AGGREGATES.iter().any(|f| p.contains(&format!("{f}(")))
                                })
                        }
                        _ => false,
                    });
                    let info = super::exec::CursorInfo {
                        name: name.clone(),
                        statement: a::Statement::Declare { stmts: vec![d.clone()] }.to_string(),
                        holdable: hold,
                        binary: d.binary == Some(true),
                        scrollable: scroll,
                        created: s.rt.stmt_now,
                    };
                    s.cursors.insert(
                        name,
                        Cursor {
                            cols: result.cols,
                            rows: result.rows,
                            pos: 0,
                            hold,
                            new: true,
                            info,
                        },
                    );
                }
                Ok(StmtResult::tag("DECLARE CURSOR"))
            }
            S::Fetch { name, direction, into, .. } => {
                if into.is_some() {
                    return Err(unsupported("FETCH ... INTO"));
                }
                let key = cursor_key(name);
                let cur = s.cursors.get_mut(&key).ok_or_else(|| {
                    PgError::new(
                        code::INVALID_CURSOR_NAME,
                        format!("cursor \"{key}\" does not exist"),
                    )
                })?;
                let n = match direction {
                    a::FetchDirection::Next => 1,
                    a::FetchDirection::Count { limit } => fetch_count(limit)?,
                    a::FetchDirection::Forward { limit: Some(l) } => fetch_count(l)?,
                    a::FetchDirection::Forward { limit: None } => 1,
                    a::FetchDirection::All | a::FetchDirection::ForwardAll => usize::MAX,
                    _ => return Err(unsupported("FETCH direction (only forward movement is)")),
                };
                let end = cur.pos.saturating_add(n).min(cur.rows.len());
                let rows = cur.rows[cur.pos..end].to_vec();
                cur.pos = end;
                let cols = cur.cols.clone();
                Ok(StmtResult {
                    cols,
                    rows,
                    tag: "FETCH".into(),
                    notices: vec![],
                    params_changed: vec![],
                    returns_rows: true,
                })
            }
            S::Close { cursor } => {
                match cursor {
                    a::CloseCursor::All => {
                        s.cursors.clear();
                        return Ok(StmtResult::tag("CLOSE CURSOR ALL"));
                    }
                    a::CloseCursor::Specific { name } => {
                        let key = cursor_key(name);
                        if s.cursors.remove(&key).is_none() {
                            return Err(PgError::new(
                                code::INVALID_CURSOR_NAME,
                                format!("cursor \"{key}\" does not exist"),
                            ));
                        }
                    }
                }
                Ok(StmtResult::tag("CLOSE CURSOR"))
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
            other => self.run_data_statement(s, other, params, param_types),
        }
    }

    /// Everything that touches the database: queries, DML and DDL.
    fn run_data_statement(
        &self,
        s: &mut Session,
        stmt: &a::Statement,
        params: &[Value],
        param_types: &[Type],
    ) -> PgResult<StmtResult> {
        // A query calling a user function may write through it.
        let writes = statement_writes(stmt)
            || (matches!(stmt, a::Statement::Query(_)) && {
                let text = stmt.to_string().to_lowercase();
                self.with_db(s, |db| {
                    db.functions.values().any(|f| text.contains(&format!("{}(", f.name)))
                })
            });
        let implicit = s.txn.is_none();
        if implicit {
            self.begin(s);
        }
        if writes {
            self.acquire_writer(s)?;
        }
        let out = self.run_in_txn(s, stmt, params, param_types);
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
        param_types: &[Type],
    ) -> PgResult<StmtResult> {
        let mut g = self.global.lock().unwrap();
        let databases_info: Vec<(u32, String)> =
            g.databases.values().map(|d| (d.oid, d.name.clone())).collect();
        let global = &mut *g;
        let global_db = global.databases.get_mut(&s.rt.database).unwrap();
        let txn = s.txn.as_mut().expect("transaction");
        let mut ctx = Ctx {
            db: &mut txn.state,
            seqs: &mut global_db.seqs,
            rt: &mut s.rt,
            params,
            outer: vec![],
            ctes: vec![],
            notifies: vec![],
            affected: 0,
            databases: databases_info,
            subq_cache: vec![],
            min_outer: usize::MAX,
        };
        let info = SessionInfo {
            user: ctx.rt.user.clone(),
            database: ctx.rt.database.clone(),
            search_path: ctx.rt.settings.lookup_path(&ctx.rt.user),
            fmt: ctx.rt.settings.fmt(),
            now: ctx.rt.now,
        };
        let mut result = run_one(&mut ctx, stmt, &info, param_types);
        // RAISE NOTICE and friends from functions and triggers.
        let raised = std::mem::take(&mut ctx.rt.notices);
        if let Ok(r) = &mut result {
            r.notices.extend(raised);
        }
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

    /// `SET CONSTRAINTS names|ALL DEFERRED|IMMEDIATE` (see
    /// `rewrite_set_constraints`): for the rest of the transaction;
    /// IMMEDIATE also checks those constraints now.
    fn set_constraints(&self, s: &mut Session, value: &str) -> PgResult<StmtResult> {
        let mut r = StmtResult::tag("SET CONSTRAINTS");
        let (names, mode) = value.rsplit_once('|').unwrap_or((value, "immediate"));
        let deferred = mode == "deferred";
        if s.status != TxStatus::InTransaction || s.txn.is_none() {
            r.notices.push(PgError {
                severity: "WARNING",
                ..PgError::new("25P01", "SET CONSTRAINTS can only be used in transaction blocks")
            });
            return Ok(r);
        }
        let state = &s.txn.as_ref().unwrap().state;
        let names: Vec<String> = names.split(',').map(str::to_string).collect();
        let all = names.len() == 1 && names[0] == "all";
        if all {
            s.rt.deferred_all = Some(deferred);
            s.rt.deferred.clear();
        } else {
            for n in &names {
                let cons =
                    state.tables.values().flat_map(|t| &t.constraints).find(|c| &c.name == n);
                match cons {
                    None => {
                        return Err(PgError::new(
                            code::UNDEFINED_OBJECT,
                            format!("constraint \"{n}\" does not exist"),
                        ));
                    }
                    Some(c) if !c.deferrable && !c.initially_deferred => {
                        return Err(PgError::new(
                            code::WRONG_OBJECT_TYPE,
                            format!("constraint \"{n}\" is not deferrable"),
                        ));
                    }
                    Some(_) => {}
                }
            }
            for n in &names {
                s.rt.deferred.insert(n.clone(), deferred);
            }
        }
        if !deferred {
            super::dml::check_deferred_named(state, (!all).then_some(&names[..]))?;
        }
        Ok(r)
    }

    fn run_set(&self, s: &mut Session, set: &a::Set) -> PgResult<StmtResult> {
        let mut changed = vec![];
        match set {
            a::Set::SingleAssignment { scope, variable, values, .. } => {
                let local = matches!(scope, Some(a::ContextModifier::Local));
                let name = name_parts(variable).join(".");
                let value = set_value_text(values)?;
                if local && s.status != TxStatus::InTransaction && name != "noida_set_constraints" {
                    // Outside a transaction block it would last only for
                    // this statement: a warning and no effect, as Postgres.
                    let mut r = StmtResult::tag("SET");
                    r.notices.push(warning("SET LOCAL can only be used in transaction blocks"));
                    return Ok(r);
                }
                if local {
                    s.rt.local_settings.push(name.clone());
                }
                if name == "noida_set_constraints" {
                    return self.set_constraints(s, &value);
                }
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
                    // `SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE`
                    // (what some drivers send for a fixed-offset zone that
                    // isn't a named one): the offset is the interval's own
                    // literal text, which already parses as one.
                    a::Expr::Interval(iv) => match &*iv.value {
                        a::Expr::Value(v) => match &v.value {
                            a::Value::SingleQuotedString(s) => s.clone(),
                            other => other.to_string(),
                        },
                        other => other.to_string(),
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
        let db_state = g.databases.get(&s.rt.database).unwrap().db.clone();
        s.rt.local_settings.clear();
        s.txn = Some(Txn {
            state: db_state,
            savepoints: vec![],
            wrote: false,
            settings: s.rt.settings.clone(),
            savepoint_settings: vec![],
        });
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
                                tx.state = g.databases.get(&s.rt.database).unwrap().db.clone();
                            }
                            tx.wrote = true;
                        }
                        return Ok(());
                    }
                }
            }
            s.rt.check_interrupt()?;
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
        // Deferred constraints are checked now; a violation rolls back.
        if let Some(tx) = s.txn.as_ref()
            && tx.wrote
            && let Err(e) = super::dml::check_deferred(&tx.state)
        {
            self.rollback(s);
            s.status = TxStatus::Idle;
            return Err(e);
        }
        let Some(mut tx) = s.txn.take() else { return Ok(()) };
        // SET LOCAL lasts only for the transaction.
        for name in std::mem::take(&mut s.rt.local_settings) {
            s.rt.settings.restore_from(&name, &tx.settings);
        }
        s.rt.deferred_all = None;
        s.rt.deferred.clear();
        if tx.wrote {
            on_commit_actions(&mut tx.state, &s.rt.settings.temp_schema);
        }
        let mut g = self.global.lock().unwrap();
        if tx.wrote
            && let Some(global_db) = g.databases.get_mut(&s.rt.database)
        {
            global_db.db = tx.state;
            crate::persistence::mark("postgres");
        }
        if g.writer == Some(s.id) {
            g.writer = None;
        }
        // WITH HOLD cursors outlive the commit.
        s.cursors.retain(|_, c| c.hold);
        for c in s.cursors.values_mut() {
            c.new = false;
        }
        Ok(())
    }

    fn rollback(&self, s: &mut Session) {
        // Settings changed in the transaction revert with it.
        if let Some(tx) = s.txn.take() {
            s.rt.settings = tx.settings;
        }
        s.rt.local_settings.clear();
        s.rt.deferred_all = None;
        s.rt.deferred.clear();
        let mut g = self.global.lock().unwrap();
        if g.writer == Some(s.id) {
            g.writer = None;
        }
        // Held cursors from earlier transactions survive; the rest go.
        s.cursors.retain(|_, c| c.hold && !c.new);
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
                if let Some(settings) = tx.savepoint_settings.get(i) {
                    s.rt.settings = settings.clone();
                }
                tx.savepoint_settings.truncate(i + 1);
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
        super::catalog::set_session_temp_schema(&s.rt.settings.temp_schema);
        match &s.txn {
            Some(tx) => f(&tx.state),
            None => {
                let g = self.global.lock().unwrap();
                f(&g.databases.get(&s.rt.database).unwrap().db)
            }
        }
    }

    /// Names for reg* values in a result set.
    pub fn reg_names(&self, s: &Session, cols: &[OutCol]) -> Option<Arc<RegNames>> {
        if !cols.iter().any(|c| c.ty.is_reg()) {
            return None;
        }
        Some(Arc::new(self.with_db(s, |db| {
            super::exec::build_reg_names(db, &s.rt.user, &s.rt.settings.lookup_path(&s.rt.user))
        })))
    }
}

/// The single string-literal argument of a `CALL x('...')` synthesized by
/// `seqddl.rs`/`refresh.rs` to carry text sqlparser can't parse directly.
/// Temporary tables' `ON COMMIT DELETE ROWS` / `ON COMMIT DROP`.
fn on_commit_actions(db: &mut DbState, temp_schema: &str) {
    use super::catalog::OnCommit;
    let Some(ns) = db.schemas.values().find(|s| s.name == temp_schema).map(|s| s.oid) else {
        return;
    };
    let mut dropped = vec![];
    for t in db.tables.values_mut() {
        if t.schema != ns {
            continue;
        }
        match t.on_commit {
            OnCommit::PreserveRows => {}
            OnCommit::DeleteRows => {
                if !t.rows.is_empty() {
                    Arc::make_mut(t).rows.clear();
                }
            }
            OnCommit::Drop => dropped.push(t.oid),
        }
    }
    db.tables.retain(|oid, _| !dropped.contains(oid));
    db.triggers.retain(|_, tr| !dropped.contains(&tr.table));
}

fn call_arg_text(f: &a::Function) -> String {
    match &f.args {
        a::FunctionArguments::List(l) => l.args.iter().find_map(|x| match x {
            a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(a::Expr::Value(v))) => {
                match &v.value {
                    a::Value::SingleQuotedString(s) => Some(s.clone()),
                    _ => None,
                }
            }
            _ => None,
        }),
        _ => None,
    }
    .unwrap_or_default()
}

fn warning(msg: &str) -> PgError {
    PgError { severity: "WARNING", ..PgError::new(code::WARNING, msg) }
}

/// The row count in `FETCH n FROM cursor` / `FETCH FORWARD n FROM cursor`.
fn fetch_count(limit: &a::ValueWithSpan) -> PgResult<usize> {
    let a::Value::Number(n, _) = &limit.value else {
        return Err(unsupported("non-numeric FETCH count"));
    };
    n.parse().map_err(|_| PgError::new(code::SYNTAX_ERROR, format!("invalid FETCH count: {n}")))
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
                vec![OutCol::new(name.clone(), Type::TEXT)]
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
pub(crate) fn run_one(
    ctx: &mut Ctx,
    stmt: &a::Statement,
    info: &SessionInfo,
    param_types: &[Type],
) -> PgResult<StmtResult> {
    use a::Statement as S;
    match stmt {
        S::Query(_) | S::Insert(_) | S::Update(_) | S::Delete(_) => {
            let db = ctx.db.clone();
            // Re-binding here needs hints for `ctx.params`; reuse the ones
            // `Prepared::param_types` already resolved (see `execute`'s
            // doc comment) rather than re-guessing from the decoded
            // values, falling back to `Unknown` (safe: this binder's own
            // "resolve `Unknown` from context" handling is exactly what
            // `prepare` itself used) only if the caller genuinely has none.
            let hints: Vec<Type> = if param_types.len() == ctx.params.len() {
                param_types.to_vec()
            } else {
                vec![Type::UNKNOWN; ctx.params.len()]
            };
            let mut b = Binder::new(&db, info, &hints);
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
            let tag = d.create_view(
                &cv.name,
                &cv.query,
                &cv.columns,
                cv.or_replace,
                cv.materialized,
                cv.temporary,
            )?;
            Ok(StmtResult::tag(tag))
        }
        S::AlterIndex { name, operation: a::AlterIndexOperation::RenameIndex { index_name } } => {
            let mut d = ddl(ctx, info);
            let tag = d.rename_index(name, index_name)?;
            Ok(StmtResult::tag(tag))
        }
        S::CreateExtension(ce) => {
            let mut d = ddl(ctx, info);
            let tag = d.create_extension(ce)?;
            Ok(StmtResult::tag(tag))
        }
        S::DropExtension(de) => {
            let mut d = ddl(ctx, info);
            let tag = d.drop_extension(de)?;
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
        // CREATE and ALTER SEQUENCE arrive as a CALL (see seqddl.rs).
        S::Call(f) if f.name.to_string() == super::seqddl::CALL_NAME => {
            let sq = super::seqddl::parse(&call_arg_text(f))?;
            let mut d = ddl(ctx, info);
            let tag = if sq.create { d.create_sequence(&sq)? } else { d.alter_sequence(&sq)? };
            Ok(StmtResult::tag(tag))
        }
        // REFRESH MATERIALIZED VIEW arrives as a CALL (see refresh.rs).
        // Functions, procedures, triggers, DO and CALL (see plpgsql.rs).
        S::Call(f) if f.name.to_string() == super::plpgsql::CALL_NAME => {
            let tag = super::plpgsql::ddl(ctx, info, &call_arg_text(f))?;
            Ok(StmtResult::tag(tag))
        }
        S::Call(f) if f.name.to_string() == super::refresh::CALL_NAME => {
            let r = super::refresh::parse(&call_arg_text(f))?;
            let mut d = ddl(ctx, info);
            let tag = d.refresh_matview(&r)?;
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
        S::AlterType(at) => {
            let mut d = ddl(ctx, info);
            let tag = d.alter_type(&at.name, &at.operation)?;
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
