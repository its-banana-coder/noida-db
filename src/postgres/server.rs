//! The wire protocol: one thread per connection, messages via pgwire's codecs.

use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use pgwire::messages::data::{DataRow, FieldDescription, ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    BindComplete, CloseComplete, ParseComplete, PortalSuspended,
};
use pgwire::messages::response::{
    CommandComplete, EmptyQueryResponse, ErrorResponse, GssEncResponse, NoticeResponse,
    NotificationResponse, ReadyForQuery, SslResponse, TransactionStatus,
};
use pgwire::messages::startup::{
    Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus, SecretKey,
};
use pgwire::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
    SslNegotiationMetaMessage,
};

use super::auth::{AuthMethod, Scram, ScramError};
use super::engine::{Engine, Portal, Prepared, Session, StmtResult, TxStatus};
use super::error::{PgError, PgResult, code};
use super::plan::OutCol;
use super::types::{self, Type, Value};

/// Connection threads touch little stack; keep reservations small.
const STACK_SIZE: usize = 1024 * 1024;

/// How noida asks clients to authenticate, and the password it expects.
#[derive(Clone)]
pub struct Config {
    pub auth: AuthMethod,
    pub password: String,
    pub database: String,
}

impl Default for Config {
    fn default() -> Self {
        let auth = std::env::var("NOIDA_POSTGRES_AUTH")
            .ok()
            .and_then(|v| AuthMethod::parse(&v))
            .unwrap_or(AuthMethod::Trust);
        Config {
            auth,
            password: std::env::var("NOIDA_POSTGRES_PASSWORD")
                .unwrap_or_else(|_| "postgres".into()),
            database: "postgres".into(),
        }
    }
}

/// Binds `addr` and serves Postgres on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    spawn_with(addr, Config::default())
}

pub fn spawn_with(addr: &str, cfg: Config) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let engine = Engine::new();
    let cfg = Arc::new(cfg);
    std::thread::Builder::new().name("postgres-accept".into()).stack_size(STACK_SIZE).spawn(
        move || {
            for stream in listener.incoming().flatten() {
                let engine = engine.clone();
                let cfg = cfg.clone();
                let _ = std::thread::Builder::new()
                    .name("postgres-conn".into())
                    .stack_size(STACK_SIZE)
                    .spawn(move || {
                        let _ = serve(stream, engine, cfg);
                    });
            }
        },
    )?;
    Ok(local)
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
    buf: BytesMut,
    out: BytesMut,
    ctx: DecodeContext,
}

impl Conn {
    fn send(&mut self, msg: PgWireBackendMessage) -> io::Result<()> {
        msg.encode(&mut self.out).map_err(io::Error::other)?;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            let bytes = self.out.split();
            self.stream.write_all(&bytes)?;
            self.stream.flush()?;
        }
        Ok(())
    }

    /// Reads the next frontend message, blocking as needed.
    fn recv(&mut self) -> io::Result<Option<PgWireFrontendMessage>> {
        loop {
            match PgWireFrontendMessage::decode(&mut self.buf, &self.ctx) {
                Ok(Some(msg)) => return Ok(Some(msg)),
                Ok(None) => {}
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            }
            self.flush()?;
            let mut chunk = [0u8; 8192];
            let n = self.reader.read(&mut chunk)?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

fn serve(stream: TcpStream, engine: Engine, cfg: Arc<Config>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut conn = Conn {
        reader: BufReader::new(stream.try_clone()?),
        stream,
        buf: BytesMut::with_capacity(8192),
        out: BytesMut::with_capacity(8192),
        ctx: DecodeContext::new(ProtocolVersion::PROTOCOL3_0),
    };
    // Startup: SSL/GSS negotiation, then the startup packet.
    let startup = loop {
        let Some(msg) = conn.recv()? else { return Ok(()) };
        match msg {
            PgWireFrontendMessage::SslNegotiation(meta) => {
                match meta {
                    SslNegotiationMetaMessage::PostgresSsl(_) => {
                        conn.send(PgWireBackendMessage::SslResponse(SslResponse::Refuse))?
                    }
                    SslNegotiationMetaMessage::PostgresGss(_) => {
                        conn.send(PgWireBackendMessage::GssEncResponse(GssEncResponse::Refuse))?
                    }
                    SslNegotiationMetaMessage::None => {}
                }
                conn.ctx.awaiting_frontend_ssl = false;
                conn.flush()?;
            }
            PgWireFrontendMessage::CancelRequest(c) => {
                let secret = c.secret_key.as_i32().unwrap_or(0);
                engine.cancel(c.pid, secret);
                return Ok(());
            }
            PgWireFrontendMessage::Startup(s) => break s,
            _ => return Ok(()),
        }
    };
    conn.ctx.awaiting_frontend_startup = false;
    if startup.protocol_number_major != 3 {
        let e = PgError::fatal(
            code::PROTOCOL_VIOLATION,
            format!(
                "unsupported frontend protocol {}.{}: server supports 3.0",
                startup.protocol_number_major, startup.protocol_number_minor
            ),
        );
        conn.send(PgWireBackendMessage::ErrorResponse(ErrorResponse::new(e.fields())))?;
        conn.flush()?;
        return Ok(());
    }
    if startup.protocol_number_minor > 0 {
        let unsupported: Vec<String> =
            startup.parameters.keys().filter(|k| k.starts_with("_pq_.")).cloned().collect();
        conn.send(PgWireBackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion::new(
            0,
            unsupported,
        )))?;
    }
    let user = startup.parameters.get("user").cloned().unwrap_or_else(|| "postgres".into());
    let database = startup.parameters.get("database").cloned().unwrap_or_else(|| user.clone());
    if let Err(e) = authenticate(&mut conn, &cfg, &user) {
        conn.send(PgWireBackendMessage::ErrorResponse(ErrorResponse::new(e.fields())))?;
        conn.flush()?;
        return Ok(());
    }
    conn.send(PgWireBackendMessage::Authentication(Authentication::Ok))?;

    let mut session = engine.connect(&user, &database);
    // Startup parameters the client asked for.
    for (k, v) in &startup.parameters {
        if matches!(k.as_str(), "user" | "database" | "client_encoding" | "options" | "replication")
        {
            if k == "client_encoding" {
                let _ = session.rt.settings.set("client_encoding", v);
            }
            continue;
        }
        let _ = session.rt.settings.set(k, v);
    }
    session.rt.settings.mark_session_defaults();
    for name in super::session::Settings::reported() {
        let value = session.rt.settings.get(name).unwrap_or_default();
        conn.send(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
            name.to_string(),
            value,
        )))?;
    }
    conn.send(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
        session.pid,
        SecretKey::I32(session.secret),
    )))?;
    conn.send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)))?;
    conn.flush()?;

    let result = main_loop(&mut conn, &engine, &mut session);
    engine.disconnect(&session);
    result
}

fn authenticate(conn: &mut Conn, cfg: &Config, user: &str) -> PgResult<()> {
    let io_err = |e: io::Error| PgError::fatal(code::PROTOCOL_VIOLATION, e.to_string());
    match cfg.auth {
        AuthMethod::Trust => Ok(()),
        AuthMethod::Password => {
            conn.send(PgWireBackendMessage::Authentication(Authentication::CleartextPassword))
                .map_err(io_err)?;
            conn.flush().map_err(io_err)?;
            let msg = conn.recv().map_err(io_err)?;
            let Some(PgWireFrontendMessage::PasswordMessageFamily(p)) = msg else {
                return Err(PgError::fatal(code::PROTOCOL_VIOLATION, "expected password message"));
            };
            let pw = p
                .into_password()
                .map_err(|e| PgError::fatal(code::PROTOCOL_VIOLATION, e.to_string()))?;
            if pw.password == cfg.password { Ok(()) } else { Err(bad_password(user)) }
        }
        AuthMethod::Md5 => {
            let salt: [u8; 4] = (super::funcs::random_u64() as u32).to_le_bytes();
            conn.send(PgWireBackendMessage::Authentication(Authentication::MD5Password(
                salt.to_vec(),
            )))
            .map_err(io_err)?;
            conn.flush().map_err(io_err)?;
            let msg = conn.recv().map_err(io_err)?;
            let Some(PgWireFrontendMessage::PasswordMessageFamily(p)) = msg else {
                return Err(PgError::fatal(code::PROTOCOL_VIOLATION, "expected password message"));
            };
            let pw = p
                .into_password()
                .map_err(|e| PgError::fatal(code::PROTOCOL_VIOLATION, e.to_string()))?;
            if pw.password == super::auth::md5_response(user, &cfg.password, salt) {
                Ok(())
            } else {
                Err(bad_password(user))
            }
        }
        AuthMethod::ScramSha256 => {
            conn.send(PgWireBackendMessage::Authentication(Authentication::SASL(vec![
                "SCRAM-SHA-256".into(),
            ])))
            .map_err(io_err)?;
            conn.flush().map_err(io_err)?;
            let mut scram = Scram::new(&cfg.password);
            let msg = conn.recv().map_err(io_err)?;
            let Some(PgWireFrontendMessage::PasswordMessageFamily(p)) = msg else {
                return Err(PgError::fatal(code::PROTOCOL_VIOLATION, "expected SASL response"));
            };
            let init = p
                .into_sasl_initial_response()
                .map_err(|e| PgError::fatal(code::PROTOCOL_VIOLATION, e.to_string()))?;
            let data = init.data.unwrap_or_default();
            let client_first = String::from_utf8_lossy(&data).to_string();
            let server_first = scram.client_first(&client_first).map_err(scram_err)?;
            conn.send(PgWireBackendMessage::Authentication(Authentication::SASLContinue(
                bytes::Bytes::from(server_first.into_bytes()),
            )))
            .map_err(io_err)?;
            conn.flush().map_err(io_err)?;
            let msg = conn.recv().map_err(io_err)?;
            let Some(PgWireFrontendMessage::PasswordMessageFamily(p)) = msg else {
                return Err(PgError::fatal(code::PROTOCOL_VIOLATION, "expected SASL response"));
            };
            let resp = p
                .into_sasl_response()
                .map_err(|e| PgError::fatal(code::PROTOCOL_VIOLATION, e.to_string()))?;
            let final_msg = String::from_utf8_lossy(&resp.data).to_string();
            let server_final = scram.client_final(&final_msg).map_err(|e| match e {
                ScramError::BadPassword => bad_password(user),
                other => scram_err(other),
            })?;
            conn.send(PgWireBackendMessage::Authentication(Authentication::SASLFinal(
                bytes::Bytes::from(server_final.into_bytes()),
            )))
            .map_err(io_err)?;
            Ok(())
        }
    }
}

fn bad_password(user: &str) -> PgError {
    PgError::fatal(
        code::INVALID_PASSWORD,
        format!("password authentication failed for user \"{user}\""),
    )
}

fn scram_err(e: ScramError) -> PgError {
    match e {
        ScramError::BadPassword => {
            PgError::fatal(code::INVALID_PASSWORD, "password authentication failed")
        }
        ScramError::Protocol(m) => PgError::fatal(code::PROTOCOL_VIOLATION, m),
    }
}

fn main_loop(conn: &mut Conn, engine: &Engine, session: &mut Session) -> io::Result<()> {
    let mut skip_until_sync = false;
    loop {
        let Some(msg) = conn.recv()? else { return Ok(()) };
        if skip_until_sync
            && !matches!(msg, PgWireFrontendMessage::Sync(_) | PgWireFrontendMessage::Terminate(_))
        {
            continue;
        }
        match msg {
            PgWireFrontendMessage::Terminate(_) => return Ok(()),
            PgWireFrontendMessage::Query(q) => {
                simple_query(conn, engine, session, &q.query)?;
                send_notifications(conn, session)?;
                ready(conn, session)?;
                conn.flush()?;
            }
            PgWireFrontendMessage::Parse(p) => {
                let name = p.name.clone().unwrap_or_default();
                match do_parse(engine, session, &name, &p.query, &p.type_oids) {
                    Ok(()) => {
                        conn.send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))?
                    }
                    Err(e) => {
                        error(conn, session, e)?;
                        skip_until_sync = true;
                    }
                }
            }
            PgWireFrontendMessage::Bind(b) => match do_bind(engine, session, &b) {
                Ok(()) => conn.send(PgWireBackendMessage::BindComplete(BindComplete::new()))?,
                Err(e) => {
                    error(conn, session, e)?;
                    skip_until_sync = true;
                }
            },
            PgWireFrontendMessage::Describe(d) => {
                let name = d.name.clone().unwrap_or_default();
                let r = if d.target_type == b'S' {
                    describe_statement(conn, engine, session, &name)
                } else {
                    describe_portal(conn, engine, session, &name)
                };
                if let Err(e) = r {
                    error(conn, session, e)?;
                    skip_until_sync = true;
                }
            }
            PgWireFrontendMessage::Execute(e) => {
                let name = e.name.clone().unwrap_or_default();
                match do_execute(conn, engine, session, &name, e.max_rows) {
                    Ok(()) => {}
                    Err(err) => {
                        error(conn, session, err)?;
                        skip_until_sync = true;
                    }
                }
            }
            PgWireFrontendMessage::Close(c) => {
                let name = c.name.clone().unwrap_or_default();
                if c.target_type == b'S' {
                    session.prepared.remove(&name);
                } else {
                    session.portals.remove(&name);
                }
                conn.send(PgWireBackendMessage::CloseComplete(CloseComplete::new()))?;
            }
            PgWireFrontendMessage::Flush(_) => conn.flush()?,
            PgWireFrontendMessage::Sync(_) => {
                skip_until_sync = false;
                if session.in_implicit_tx {
                    if session.status == TxStatus::Failed {
                        engine.rollback_implicit(session);
                    } else if let Err(e) = engine.commit_implicit(session) {
                        error(conn, session, e)?;
                    }
                }
                send_notifications(conn, session)?;
                ready(conn, session)?;
                conn.flush()?;
            }
            PgWireFrontendMessage::CopyData(_) | PgWireFrontendMessage::CopyDone(_) => {}
            PgWireFrontendMessage::CopyFail(_) => {
                error(conn, session, PgError::new(code::QUERY_CANCELED, "COPY from stdin failed"))?;
            }
            other => {
                let _ = other;
                error(
                    conn,
                    session,
                    PgError::new(
                        code::PROTOCOL_VIOLATION,
                        "unexpected message in the connection state",
                    ),
                )?;
            }
        }
    }
}

fn ready(conn: &mut Conn, session: &Session) -> io::Result<()> {
    let status = match session.status {
        TxStatus::Idle => TransactionStatus::Idle,
        TxStatus::InTransaction => TransactionStatus::Transaction,
        TxStatus::Failed => TransactionStatus::Error,
    };
    conn.send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(status)))
}

fn error(conn: &mut Conn, session: &mut Session, e: PgError) -> io::Result<()> {
    if session.status == super::engine::TxStatus::InTransaction {
        session.status = super::engine::TxStatus::Failed;
    }
    conn.send(PgWireBackendMessage::ErrorResponse(ErrorResponse::new(e.fields())))
}

fn notices(conn: &mut Conn, list: &[PgError]) -> io::Result<()> {
    for n in list {
        conn.send(PgWireBackendMessage::NoticeResponse(NoticeResponse::new(n.fields())))?;
    }
    Ok(())
}

fn send_notifications(conn: &mut Conn, session: &mut Session) -> io::Result<()> {
    let pending: Vec<(i32, String, String)> = {
        let mut q = session.notifications.lock().unwrap();
        std::mem::take(&mut *q)
    };
    for (pid, channel, payload) in pending {
        conn.send(PgWireBackendMessage::NotificationResponse(NotificationResponse::new(
            pid, channel, payload,
        )))?;
    }
    Ok(())
}

fn params_changed(conn: &mut Conn, changed: &[(String, String)]) -> io::Result<()> {
    for (k, v) in changed {
        conn.send(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
            k.clone(),
            v.clone(),
        )))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Simple query

fn simple_query(
    conn: &mut Conn,
    engine: &Engine,
    session: &mut Session,
    sql: &str,
) -> io::Result<()> {
    let stmts = match engine.parse_sql(sql) {
        Ok(s) => s,
        Err(e) => return error(conn, session, e),
    };
    if stmts.is_empty() {
        return conn.send(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse::new()));
    }
    if stmts.len() > 1 {
        engine.begin_implicit(session);
    }
    for stmt in &stmts {
        match engine.execute(session, stmt, &[]) {
            Ok(result) => {
                notices(conn, &result.notices)?;
                params_changed(conn, &result.params_changed)?;
                if result.returns_rows {
                    let fmt = session.rt.settings.fmt();
                    let reg = engine.reg_names(session, &result.cols);
                    let fmt = types::FmtCtx { reg_names: reg, ..fmt };
                    conn.send(PgWireBackendMessage::RowDescription(row_description(
                        &result.cols,
                        &[],
                    )))?;
                    for row in &result.rows {
                        conn.send(PgWireBackendMessage::DataRow(data_row(
                            row,
                            &result.cols,
                            &[],
                            &fmt,
                        )))?;
                    }
                }
                conn.send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    result.tag.clone(),
                )))?;
            }
            Err(e) => {
                error(conn, session, e)?;
                if session.in_implicit_tx {
                    engine.rollback_implicit(session);
                }
                return Ok(());
            }
        }
    }
    if session.in_implicit_tx
        && let Err(e) = engine.commit_implicit(session)
    {
        error(conn, session, e)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Extended query

fn do_parse(
    engine: &Engine,
    session: &mut Session,
    name: &str,
    sql: &str,
    oids: &[u32],
) -> PgResult<()> {
    if !name.is_empty() && session.prepared.contains_key(name) {
        return Err(PgError::new(
            code::DUPLICATE_PSTATEMENT,
            format!("prepared statement \"{name}\" already exists"),
        ));
    }
    let hints: Vec<Type> = oids
        .iter()
        .map(|o| if *o == 0 { Type::UNKNOWN } else { Type::from_oid(*o).unwrap_or(Type::TEXT) })
        .collect();
    let prep = engine.prepare(session, sql, &hints)?;
    session.prepared.insert(name.to_string(), prep);
    Ok(())
}

fn do_bind(
    engine: &Engine,
    session: &mut Session,
    b: &pgwire::messages::extendedquery::Bind,
) -> PgResult<()> {
    let stmt_name = b.statement_name.clone().unwrap_or_default();
    let portal_name = b.portal_name.clone().unwrap_or_default();
    let prep = session.prepared.get(&stmt_name).cloned().ok_or_else(|| {
        PgError::new(
            code::UNDEFINED_PSTATEMENT,
            format!("prepared statement \"{stmt_name}\" does not exist"),
        )
    })?;
    let formats = &b.parameter_format_codes;
    let mut values = vec![];
    for (i, raw) in b.parameters.iter().enumerate() {
        let ty = prep.param_types.get(i).copied().unwrap_or(Type::TEXT);
        let format = match formats.len() {
            0 => 0,
            1 => formats[0],
            _ => *formats.get(i).unwrap_or(&0),
        };
        values.push(match raw {
            None => Value::Null,
            Some(bytes) => {
                if format == 1 {
                    types::from_binary(bytes, ty)?
                } else {
                    let s = std::str::from_utf8(bytes).map_err(|_| {
                        PgError::new(
                            code::CHARACTER_NOT_IN_REPERTOIRE,
                            "invalid byte sequence for encoding \"UTF8\"",
                        )
                    })?;
                    let dctx = super::datetime::Ctx {
                        now: session.rt.now,
                        zone: &session.rt.settings.fmt().zone,
                    };
                    types::from_text(s, ty, &dctx)?
                }
            }
        });
    }
    if values.len() < prep.param_types.len() {
        return Err(PgError::new(
            code::PROTOCOL_VIOLATION,
            format!(
                "bind message supplies {} parameters, but prepared statement \"{stmt_name}\" requires {}",
                values.len(),
                prep.param_types.len()
            ),
        ));
    }
    let _ = engine;
    session.portals.insert(
        portal_name,
        Portal {
            statement: stmt_name,
            params: values,
            result_formats: b.result_column_format_codes.clone(),
            cols: prep.cols.clone(),
            rows: vec![],
            pos: 0,
            tag: String::new(),
            executed: false,
            returns_rows: prep.returns_rows,
            suspended: false,
        },
    );
    Ok(())
}

fn describe_statement(
    conn: &mut Conn,
    engine: &Engine,
    session: &mut Session,
    name: &str,
) -> PgResult<()> {
    let prep = session.prepared.get(name).cloned().ok_or_else(|| {
        PgError::new(
            code::UNDEFINED_PSTATEMENT,
            format!("prepared statement \"{name}\" does not exist"),
        )
    })?;
    let oids: Vec<u32> = prep.param_types.iter().map(|t| t.oid()).collect();
    conn.send(PgWireBackendMessage::ParameterDescription(ParameterDescription::new(oids)))
        .map_err(io_to_pg)?;
    let _ = engine;
    if prep.returns_rows && !prep.cols.is_empty() {
        conn.send(PgWireBackendMessage::RowDescription(row_description(&prep.cols, &[])))
            .map_err(io_to_pg)?;
    } else {
        conn.send(PgWireBackendMessage::NoData(pgwire::messages::data::NoData::new()))
            .map_err(io_to_pg)?;
    }
    Ok(())
}

fn describe_portal(
    conn: &mut Conn,
    engine: &Engine,
    session: &mut Session,
    name: &str,
) -> PgResult<()> {
    let portal = session.portals.get(name).ok_or_else(|| {
        PgError::new(code::INVALID_CURSOR_NAME, format!("portal \"{name}\" does not exist"))
    })?;
    let _ = engine;
    if portal.returns_rows && !portal.cols.is_empty() {
        let cols = portal.cols.clone();
        let formats = portal.result_formats.clone();
        conn.send(PgWireBackendMessage::RowDescription(row_description(&cols, &formats)))
            .map_err(io_to_pg)?;
    } else {
        conn.send(PgWireBackendMessage::NoData(pgwire::messages::data::NoData::new()))
            .map_err(io_to_pg)?;
    }
    Ok(())
}

fn io_to_pg(e: io::Error) -> PgError {
    PgError::new(code::INTERNAL_ERROR, e.to_string())
}

fn do_execute(
    conn: &mut Conn,
    engine: &Engine,
    session: &mut Session,
    name: &str,
    max_rows: i32,
) -> PgResult<()> {
    let Some(portal) = session.portals.get(name) else {
        return Err(PgError::new(
            code::INVALID_CURSOR_NAME,
            format!("portal \"{name}\" does not exist"),
        ));
    };
    if !portal.executed {
        let stmt_name = portal.statement.clone();
        let params = portal.params.clone();
        let prep: Prepared = session.prepared.get(&stmt_name).cloned().ok_or_else(|| {
            PgError::new(
                code::UNDEFINED_PSTATEMENT,
                format!("prepared statement \"{stmt_name}\" does not exist"),
            )
        })?;
        let Some(stmt) = prep.stmt.clone() else {
            session.portals.remove(name);
            conn.send(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse::new()))
                .map_err(io_to_pg)?;
            return Ok(());
        };
        engine.begin_implicit(session);
        let result: StmtResult = engine.execute(session, &stmt, &params)?;
        notices(conn, &result.notices).map_err(io_to_pg)?;
        params_changed(conn, &result.params_changed).map_err(io_to_pg)?;
        let portal = session.portals.get_mut(name).unwrap();
        portal.cols = result.cols;
        portal.rows = result.rows;
        portal.tag = result.tag;
        portal.returns_rows = result.returns_rows;
        portal.executed = true;
        portal.pos = 0;
    }
    let fmt = session.rt.settings.fmt();
    let cols = session.portals[name].cols.clone();
    let reg = engine.reg_names(session, &cols);
    let fmt = types::FmtCtx { reg_names: reg, ..fmt };
    let portal = session.portals.get_mut(name).unwrap();
    let limit = if max_rows <= 0 { usize::MAX } else { max_rows as usize };
    let mut sent = 0;
    if portal.returns_rows {
        let formats = portal.result_formats.clone();
        while portal.pos < portal.rows.len() && sent < limit {
            let row = portal.rows[portal.pos].clone();
            portal.pos += 1;
            sent += 1;
            conn.send(PgWireBackendMessage::DataRow(data_row(&row, &cols, &formats, &fmt)))
                .map_err(io_to_pg)?;
        }
        if portal.pos < portal.rows.len() {
            conn.send(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
                .map_err(io_to_pg)?;
            return Ok(());
        }
    }
    let tag = if portal.returns_rows && portal.tag.starts_with("SELECT") {
        format!("SELECT {}", portal.rows.len())
    } else {
        portal.tag.clone()
    };
    conn.send(PgWireBackendMessage::CommandComplete(CommandComplete::new(tag)))
        .map_err(io_to_pg)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Row encoding

fn row_description(cols: &[OutCol], formats: &[i16]) -> RowDescription {
    let fields = cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let format = match formats.len() {
                0 => 0,
                1 => formats[0],
                _ => *formats.get(i).unwrap_or(&0),
            };
            FieldDescription::new(
                c.name.clone(),
                c.table_oid as i32,
                c.attnum,
                c.ty.oid(),
                c.ty.typlen(),
                c.typmod,
                format,
            )
        })
        .collect();
    RowDescription::new(fields)
}

fn data_row(row: &[Value], cols: &[OutCol], formats: &[i16], fmt: &types::FmtCtx) -> DataRow {
    let mut buf = BytesMut::new();
    let n = cols.len().max(row.len());
    for i in 0..n {
        let v = row.get(i).unwrap_or(&Value::Null);
        let ty = cols.get(i).map(|c| c.ty).unwrap_or(Type::TEXT);
        if v.is_null() {
            buf.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        let format = match formats.len() {
            0 => 0,
            1 => formats[0],
            _ => *formats.get(i).unwrap_or(&0),
        };
        let bytes = if format == 1 {
            types::to_binary(v, ty, fmt)
        } else {
            types::to_text(v, ty, fmt).into_bytes()
        };
        buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        buf.extend_from_slice(&bytes);
    }
    let field_count = n as i16;
    let mut data = BytesMut::new();
    data.extend_from_slice(&buf);
    let _ = buf.remaining();
    DataRow::new(data, field_count)
}
