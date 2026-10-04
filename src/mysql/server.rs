use crate::mysql::binder::Binder;
use crate::mysql::catalog::DbState;
use crate::mysql::engine::Engine;
use crate::mysql::error::MySqlError;
use crate::mysql::plan::{self, Plan};
use crate::mysql::types::Value;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::thread;

/// Binds `addr`, loads a snapshot from `data_dir/mysql.json` if one exists,
/// and registers a shutdown hook (via `crate::persistence::on_shutdown`) to
/// save one back on a clean exit.
pub fn spawn_persistent(addr: &str, data_dir: &Path) -> io::Result<SocketAddr> {
    let (addr, save) = spawn_persistent_for_test(addr, data_dir)?;
    crate::persistence::on_shutdown(save);
    Ok(addr)
}

/// Like `spawn_persistent`, but also returns the save closure so tests can
/// trigger a save directly without going through the process-wide shutdown
/// hook (which is unsafe to trigger from a single test once multiple
/// persistent services exist in the same test binary).
pub fn spawn_persistent_for_test(
    addr: &str,
    data_dir: &Path,
) -> io::Result<(SocketAddr, impl Fn() + Send + Sync + 'static)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;

    let path = data_dir.join("mysql.json");
    let engine = if path.exists() {
        let bytes = std::fs::read(&path)?;
        let db: DbState = serde_json::from_slice(&bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse mysql snapshot: {e}"),
            )
        })?;
        Engine::new_persistent(db)
    } else {
        Engine::new()
    };

    let save_engine = engine.clone();
    let save = move || {
        let db = save_engine.snapshot();
        let bytes = serde_json::to_vec(&db).expect("mysql snapshot serialization failed");
        let _ = crate::persistence::write_snapshot_atomically(&path, &bytes);
    };

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.new_connection();
            let _ = thread::spawn(move || {
                let _ = serve(stream, engine);
            });
        }
    });
    Ok((local, save))
}

pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    // One `Engine` for the whole listener, shared (via its internal
    // `Arc<Mutex<DbState>>`) across every connection by cloning it per
    // connection -- each connection gets its own session-local fields
    // (`current_db`, `last_insert_id`, ...) but the same underlying
    // database, matching every other service here (see e.g.
    // `postgres::server::spawn_with`). Building a fresh `Engine::new()`
    // per connection instead, as this used to do, silently gave every
    // connection its own empty, unshared database -- invisible to any
    // test that only ever used one connection, but fatal for any real
    // app: WordPress's `wp core install` (one PHP process/connection)
    // followed by `wp db tables` (a separate one) saw "the site you have
    // requested is not installed" because the second connection's tables
    // were genuinely empty.
    let engine = Engine::new();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.clone();
            let _ = thread::spawn(move || {
                let _ = serve(stream, engine);
            });
        }
    });
    Ok(local)
}

struct Session {
    engine: Engine,
    stmt_id_counter: u32,
    prepared_stmts: HashMap<u32, (String, Plan, u16)>, // ID -> (sql, Plan, num_params)
    // Parameter type codes from the last `COM_STMT_EXECUTE` that actually
    // sent them (the `new-params-bound-flag` byte), keyed by stmt id. A
    // real client is free to omit resending types on a later `EXECUTE` of
    // the same statement, reusing what it sent before.
    stmt_param_types: HashMap<u32, Vec<(u8, u8)>>,
}

/// A connection that goes away mid-transaction has it rolled back, as
/// MySQL does -- however the connection ends.
impl Drop for Session {
    fn drop(&mut self) {
        if self.engine.in_tx {
            self.engine.rollback();
        }
    }
}

fn serve(mut stream: TcpStream, engine: Engine) -> io::Result<()> {
    // Every real packet here is sent as two separate writes (a 4-byte
    // length header, then the payload) -- without this, Nagle's
    // algorithm holds the second write back waiting to coalesce with
    // more outbound data, and the client's delayed-ACK timer (a stock
    // Linux default, ~40ms) is what finally releases it. The result is a
    // real, measured ~1000x latency regression on simple point queries
    // (50ms vs Redis's ~65us for the same shape of request) -- found via
    // benchmarks/noidadb_bench.py, not a micro-optimization guess. Every
    // other service's own server that already had this (Postgres, Redis,
    // Memcached) doesn't show this problem.
    stream.set_nodelay(true)?;
    let mut session = Session {
        engine,
        stmt_id_counter: 1,
        prepared_stmts: HashMap::new(),
        stmt_param_types: HashMap::new(),
    };

    // Send Handshake: protocol 10, server version "8.0.33" -- the same
    // version `SELECT VERSION()`/`@@version` report. Found via testing
    // before a public release: the greeting used to claim
    // "5.5.5-10.4.22-MariaDB", so SQLAlchemy, Doctrine and Prisma (which
    // read it) chose their MariaDB SQL dialects. Capability flags (lower 2 bytes `\xdf\xf7` = 0xf7df,
    // upper 2 bytes `\x0f\x00` = 0x000f) advertise everything this server
    // actually does *except* CLIENT_SSL (0x0800), CLIENT_COMPRESS (0x0020)
    // and CLIENT_SSL_VERIFY_SERVER_CERT / CLIENT_REMEMBER_OPTIONS (upper
    // 0xc000). Found via testing before a public release: this used to send
    // 0xffff / 0xc00f -- claiming TLS support it doesn't have -- so any
    // client whose default is "use SSL if the server offers it" (pymysql,
    // the stock `mysql` CLI's `--ssl-mode=PREFERRED`) started a TLS
    // handshake against a plaintext server and failed to connect at all.
    let handshake = b"\x0a\x38\x2e\x30\x2e\x33\x33\x00\x01\x00\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x00\xdf\xf7\x21\x02\x00\x0f\x00\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x39\x30\x31\x32\x00\x6d\x79\x73\x71\x6c\x5f\x6e\x61\x74\x69\x76\x65\x5f\x70\x61\x73\x73\x77\x6f\x72\x64\x00";
    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(handshake.len() as u32).to_le_bytes()[0..3]);
    header[3] = 0;
    stream.write_all(&header)?;
    stream.write_all(handshake)?;

    // Read response
    let mut header = [0u8; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;

    // A client that names a database directly in its connection string
    // (`mysql://user@host/dbname`, `mysqli_real_connect(..., $dbname)`,
    // every real driver's own convention) sends it as part of this
    // handshake response, not as a separate `COM_INIT_DB` -- without this,
    // only a client that goes on to issue an explicit `USE dbname`
    // afterward (as this project's own test helpers were doing) would ever
    // see a database selected at all, and every other real client would
    // hit "No database selected" on its very first query.
    if let Some(db) = handshake_response_database(&payload) {
        session.engine.use_db(&db);
    }

    // Basic password validation placeholder according to docs
    let ok = b"\x00\x00\x00\x02\x00\x00\x00";
    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(ok.len() as u32).to_le_bytes()[0..3]);
    header[3] = 2;
    stream.write_all(&header)?;
    stream.write_all(ok)?;

    loop {
        let mut header = [0u8; 4];
        if stream.read_exact(&mut header).is_err() {
            break;
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let seq = header[3];
        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).is_err() {
            break;
        }

        if len == 0 {
            continue;
        }

        match payload[0] {
            0x01 => break, // Quit
            0x02 => {
                // Init DB
                let db = String::from_utf8_lossy(&payload[1..]);
                session.engine.use_db(&db);
                let _ = send_ok(&mut stream, seq.wrapping_add(1), 0, 0);
            }
            0x03 => {
                // Query
                let q = String::from_utf8_lossy(&payload[1..]);
                let result = session.engine.execute(&q);
                STATUS.with(|s| s.set(session.engine.status_flags()));
                match result {
                    Ok(rows) => {
                        let affected = session.engine.last_affected_rows;
                        let insert_id = session.engine.last_insert_id;
                        let names = session.engine.last_column_names.clone();
                        let _ = send_resultset(
                            &mut stream,
                            seq.wrapping_add(1),
                            rows,
                            affected,
                            insert_id,
                            &names,
                        );
                    }
                    Err(e) => {
                        let _ = send_err(
                            &mut stream,
                            seq.wrapping_add(1),
                            e.code,
                            e.sql_state,
                            &e.message,
                        );
                    }
                }
            }
            0x16 => {
                // Stmt Prepare
                let sql = String::from_utf8_lossy(&payload[1..]).to_string();
                let dialect = MySqlDialect {};
                match Parser::parse_sql(&dialect, &sql) {
                    Ok(mut asts) => {
                        if asts.is_empty() {
                            let _ = send_err(
                                &mut stream,
                                seq.wrapping_add(1),
                                1065,
                                "42000",
                                "Query was empty",
                            );
                            continue;
                        }
                        let stmt = asts.remove(0);
                        let mut binder =
                            Binder::new(session.engine.current_db.clone()).with_sql(&sql);
                        match binder.bind_statement(stmt) {
                            Ok(plan) => {
                                let stmt_id = session.stmt_id_counter;
                                session.stmt_id_counter += 1;

                                let num_params = plan::count_params(&plan)
                                    .max(binder.placeholder_count())
                                    as u16;
                                let num_columns: u16 = 0; // Simplified

                                session
                                    .prepared_stmts
                                    .insert(stmt_id, (sql.clone(), plan, num_params));

                                let mut prep_ok = Vec::new();
                                prep_ok.push(0x00);
                                prep_ok.extend_from_slice(&stmt_id.to_le_bytes());
                                prep_ok.extend_from_slice(&num_columns.to_le_bytes());
                                prep_ok.extend_from_slice(&num_params.to_le_bytes());
                                prep_ok.push(0x00); // filter
                                prep_ok.extend_from_slice(&[0x00, 0x00]); // warnings
                                let mut next_seq = seq.wrapping_add(1);
                                let _ = write_packet(&mut stream, next_seq, &prep_ok);
                                next_seq = next_seq.wrapping_add(1);

                                // Real clients (e.g. mysql_async) parse the
                                // protocol strictly: reporting a non-zero
                                // `num_params` above commits us to actually
                                // sending that many parameter-definition
                                // packets (content is otherwise unused by
                                // clients — the real type comes from
                                // COM_STMT_EXECUTE's own type codes) plus a
                                // closing EOF, or the client blocks forever
                                // waiting for them.
                                for _ in 0..num_params {
                                    let coldef = column_def_packet("?", None);
                                    let _ = write_packet(&mut stream, next_seq, &coldef);
                                    next_seq = next_seq.wrapping_add(1);
                                }
                                if num_params > 0 {
                                    let st = STATUS.with(|s| s.get()).to_le_bytes();
                                    let eof = [0xfe, 0x00, 0x00, st[0], st[1]];
                                    let _ = write_packet(&mut stream, next_seq, &eof);
                                }
                            }
                            Err(e) => {
                                let _ = send_err(
                                    &mut stream,
                                    seq.wrapping_add(1),
                                    e.code,
                                    e.sql_state,
                                    &e.message,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        let _ = send_err(
                            &mut stream,
                            seq.wrapping_add(1),
                            1064,
                            "42000",
                            &e.to_string(),
                        );
                    }
                }
            }
            0x17 => {
                // Stmt Execute: header is
                // 1 (command) + 4 (stmt-id) + 1 (flags) + 4 (iteration-count),
                // then — only if num_params > 0 — a null-bitmap, a
                // new-params-bound-flag byte, optionally per-parameter type
                // codes, then the binary-protocol-encoded values.
                if payload.len() < 10 {
                    let _ = send_err(
                        &mut stream,
                        seq.wrapping_add(1),
                        1064,
                        "42000",
                        "Malformed packet",
                    );
                    continue;
                }
                let stmt_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                let mut pos = 10;

                let Some((_sql, stmt_plan, num_params)) =
                    session.prepared_stmts.get(&stmt_id).cloned()
                else {
                    let _ = send_err(
                        &mut stream,
                        seq.wrapping_add(1),
                        1243,
                        "HY000",
                        "Unknown prepared statement handler",
                    );
                    continue;
                };

                match decode_execute_params(
                    &payload,
                    &mut pos,
                    stmt_id,
                    num_params as usize,
                    &mut session,
                ) {
                    Ok(params) => {
                        let mut executor = crate::mysql::exec::Executor::new(
                            session.engine.db.clone(),
                            session.engine.current_db.clone(),
                        );
                        executor.params = params;
                        executor.last_found_rows = session.engine.last_found_rows;
                        executor.session_insert_id = session.engine.session_insert_id;
                        executor.sql_mode = session.engine.sql_mode.clone();
                        executor.autocommit = session.engine.autocommit;
                        let names = {
                            let state = session.engine.db.lock().unwrap();
                            plan::column_names(&stmt_plan, &state)
                        };
                        let written = session.engine.before_plan(&stmt_plan);
                        let result = executor.execute_plan(stmt_plan);
                        if result.is_ok() {
                            session.engine.after_plan(written);
                        }
                        STATUS.with(|s| s.set(session.engine.status_flags()));
                        match result {
                            Ok(rows) => {
                                let affected = executor.last_affected_rows;
                                session.engine.last_affected_rows = affected;
                                session.engine.last_insert_id = executor.last_insert_id;
                                if executor.last_insert_id != 0 {
                                    session.engine.session_insert_id = executor.last_insert_id;
                                }
                                session.engine.last_found_rows = executor.last_found_rows;
                                // COM_STMT_EXECUTE's result set uses the
                                // binary protocol row format, not the text
                                // protocol format `send_resultset` (used for
                                // COM_QUERY) sends — a real client (verified
                                // against `mysql_async`) misreads the bytes
                                // (silently, as garbage/NULL values, not an
                                // error) if the two are mixed up.
                                let insert_id = executor.last_insert_id;
                                let _ = send_binary_resultset(
                                    &mut stream,
                                    seq.wrapping_add(1),
                                    rows,
                                    affected,
                                    insert_id,
                                    &names,
                                );
                            }
                            Err(e) => {
                                let _ = send_err(
                                    &mut stream,
                                    seq.wrapping_add(1),
                                    e.code,
                                    e.sql_state,
                                    &e.message,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        let _ = send_err(
                            &mut stream,
                            seq.wrapping_add(1),
                            e.code,
                            e.sql_state,
                            &e.message,
                        );
                    }
                }
            }
            0x19 => {
                // Stmt Close
                if payload.len() >= 5 {
                    let stmt_id =
                        u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    session.prepared_stmts.remove(&stmt_id);
                }
            }
            0x1a => {
                // Stmt Reset
                let _ = send_ok(&mut stream, seq.wrapping_add(1), 0, 0);
            }
            _ => {
                let _ = send_ok(&mut stream, seq.wrapping_add(1), 0, 0);
            }
        }
    }

    Ok(())
}

/// Extracts the database name a client's handshake response packet names,
/// if `CLIENT_CONNECT_WITH_DB` (flag `0x00000008`) is set -- i.e. the
/// client connected with a database already named in its connection
/// string, the way every real driver does it, rather than via a later
/// explicit `USE`. Returns `None` for a malformed packet or one that
/// doesn't set the flag at all, in either case leaving the connection with
/// no database selected (exactly as if this parsing weren't attempted).
///
/// Packet layout (protocol 4.1): 4-byte client flags, 4-byte max packet
/// size, 1-byte charset, 23 filler bytes, NUL-terminated username, then
/// the auth response (length-encoded if `CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA`
/// is set, 1-byte-length-prefixed if `CLIENT_SECURE_CONNECTION` is set,
/// NUL-terminated otherwise), then -- only if `CLIENT_CONNECT_WITH_DB` is
/// set -- a NUL-terminated database name.
fn handshake_response_database(payload: &[u8]) -> Option<String> {
    const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;

    if payload.len() < 33 {
        return None;
    }
    let client_flags = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    if client_flags & CLIENT_CONNECT_WITH_DB == 0 {
        return None;
    }

    let mut offset = 32;
    let username_end = payload[offset..].iter().position(|&b| b == 0)? + offset;
    offset = username_end + 1;

    if client_flags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let (auth_len, n) = read_lenenc_int(&payload[offset..])?;
        offset += n + auth_len as usize;
    } else if client_flags & CLIENT_SECURE_CONNECTION != 0 {
        let auth_len = *payload.get(offset)? as usize;
        offset += 1 + auth_len;
    } else {
        let auth_end = payload[offset..].iter().position(|&b| b == 0)? + offset;
        offset = auth_end + 1;
    }

    let db_end = payload[offset..].iter().position(|&b| b == 0)? + offset;
    Some(String::from_utf8_lossy(&payload[offset..db_end]).into_owned())
}

fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> io::Result<()> {
    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(payload.len() as u32).to_le_bytes()[0..3]);
    header[3] = seq;
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    Ok(())
}

thread_local! {
    /// The status flags (`SERVER_STATUS_IN_TRANS`/`_AUTOCOMMIT`) for this
    /// connection's next OK/EOF packet. One thread serves one connection,
    /// so a thread-local is per-session; it's set after every command.
    static STATUS: std::cell::Cell<u16> = const { std::cell::Cell::new(2) };
}

fn send_ok(
    stream: &mut TcpStream,
    seq: u8,
    affected_rows: u64,
    last_insert_id: u64,
) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(0x00);
    write_lenenc_int(&mut payload, affected_rows);
    write_lenenc_int(&mut payload, last_insert_id);
    payload.extend_from_slice(&STATUS.with(|s| s.get()).to_le_bytes()); // status flags
    payload.extend_from_slice(&0u16.to_le_bytes()); // warnings
    write_packet(stream, seq, &payload)
}

fn send_err(
    stream: &mut TcpStream,
    seq: u8,
    code: u16,
    sql_state: &str,
    msg: &str,
) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(0xff);
    payload.extend_from_slice(&code.to_le_bytes());
    payload.push(b'#');

    let state_bytes = sql_state.as_bytes();
    if state_bytes.len() == 5 {
        payload.extend_from_slice(state_bytes);
    } else {
        payload.extend_from_slice(b"HY000"); // fallback
    }

    payload.extend_from_slice(msg.as_bytes());
    write_packet(stream, seq, &payload)
}

/// The wire type (and charset) a text-protocol result column is declared
/// as, from a sample of its values. Found via testing before a public
/// release: everything that wasn't an Int/Float used to be declared
/// VAR_STRING, so a driver handed DECIMAL/DATE/DATETIME values back as
/// plain strings (pymysql: `'12.50'`, `'2024-03-05'`) instead of `Decimal`/
/// `date`/`datetime`, and only row 0 was sampled, so a NULL there typed the
/// whole column as a string.
fn column_type_for(val: Option<&Value>) -> (u8, u16, u8) {
    const BINARY: u16 = 0x3f;
    const UTF8MB4: u16 = 0x2d;
    match val {
        Some(Value::Int(_)) => (0x08, BINARY, 0), // LONGLONG (i64 storage)
        Some(Value::Bool(_)) => (0x01, BINARY, 0), // TINY
        Some(Value::Float(_)) => (0x05, BINARY, 31), // DOUBLE, 31 = not fixed
        Some(Value::Num(n)) => (0xf6, BINARY, n.scale().min(30) as u8), // NEWDECIMAL
        Some(Value::Date(_)) => (0x0a, BINARY, 0), // DATE
        Some(Value::Ts(_)) => (0x0c, BINARY, 0),  // DATETIME
        Some(Value::Time(_)) => (0x0b, BINARY, 0), // TIME
        Some(Value::Json(_)) => (0xf5, UTF8MB4, 0), // JSON
        Some(Value::Bytes(_)) => (0xfc, BINARY, 0), // BLOB
        _ => (0xfd, UTF8MB4, 0),                  // VAR_STRING
    }
}

fn column_def_packet(name: &str, val: Option<&Value>) -> Vec<u8> {
    let (col_type, charset, decimals) = column_type_for(val);
    let mut p = Vec::new();
    p.push(3);
    p.extend_from_slice(b"def"); // catalog
    p.push(0); // schema
    p.push(0); // table
    p.push(0); // org_table
    write_lenenc_int(&mut p, name.len() as u64);
    p.extend_from_slice(name.as_bytes()); // name
    p.push(0); // org_name
    p.push(0x0c); // length of fixed-length fields below (always 12)
    p.extend_from_slice(&charset.to_le_bytes()); // character_set (2)
    p.extend_from_slice(&255u32.to_le_bytes()); // column_length (4)
    p.push(col_type); // type (1)
    p.extend_from_slice(&0u16.to_le_bytes()); // flags (2)
    p.push(decimals); // decimals (1)
    p.extend_from_slice(&[0, 0]); // filler (2, reserved)
    p
}

/// The first non-NULL value in column `i` across every row -- what decides
/// the column's declared wire type.
fn column_sample(rows: &[Vec<Value>], i: usize) -> Option<&Value> {
    let mut vals = rows.iter().filter_map(|r| r.get(i)).filter(|v| !v.is_null());
    let first = vals.next()?;
    // A column whose rows hold different kinds of value (`CASE` branches,
    // `COALESCE(int_col, 'n/a')`, DESCRIBE's Default column) is declared as
    // a string: declaring it after the first row's type made drivers decode
    // later rows with the wrong type (pymysql raised mid-result and the
    // connection's packet stream was left out of sync).
    let kind = std::mem::discriminant(first);
    if vals.all(|v| std::mem::discriminant(v) == kind) { Some(first) } else { None }
}

/// How many result columns to declare: the rows' own width, or -- for a
/// zero-row result -- the statement's own column list. Found via testing
/// before a public release: a SELECT matching no rows used to send a bare
/// OK packet with no column definitions at all, so drivers reported no
/// result columns (pymysql's `cursor.description` was `None`) for the
/// perfectly ordinary "no matches" case. `None` means the statement
/// doesn't produce a result set (DML/DDL), so an OK packet is right.
fn result_width(rows: &[Vec<Value>], names: &[String]) -> Option<usize> {
    match rows.first() {
        Some(r) => Some(r.len()),
        None if !names.is_empty() => Some(names.len()),
        None => None,
    }
}

fn send_resultset(
    stream: &mut TcpStream,
    mut seq: u8,
    rows: Vec<Vec<Value>>,
    affected_rows: u64,
    last_insert_id: u64,
    names: &[String],
) -> io::Result<()> {
    let Some(cols) = result_width(&rows, names) else {
        return send_ok(stream, seq, affected_rows, last_insert_id);
    };
    write_packet(stream, seq, &[cols as u8])?;
    seq = seq.wrapping_add(1);

    for i in 0..cols {
        // Real column names ("col{i}" only as a last-resort fallback --
        // e.g. a computed expression like `1+1` with no alias, whose real
        // MySQL label is its own source text, not reconstructed here).
        let fallback = format!("col{i}");
        let name = names.get(i).filter(|n| n.as_str() != "?").unwrap_or(&fallback);
        let sample = column_sample(&rows, i);
        let coldef = column_def_packet(name, sample);
        write_packet(stream, seq, &coldef)?;
        seq = seq.wrapping_add(1);
    }

    let st = STATUS.with(|s| s.get()).to_le_bytes();
    let eof = [0xfe, 0x00, 0x00, st[0], st[1]];
    write_packet(stream, seq, &eof)?;
    seq = seq.wrapping_add(1);

    for row in rows {
        let mut row_payload = Vec::new();
        for val in row {
            if val.is_null() {
                row_payload.push(0xfb);
            } else {
                encode_lenenc_value(&mut row_payload, &val);
            }
        }
        write_packet(stream, seq, &row_payload)?;
        seq = seq.wrapping_add(1);
    }

    write_packet(stream, seq, &eof)?;

    Ok(())
}

/// Length-encodes one non-NULL value as text (a length-encoded string),
/// the representation both the text protocol and (with `VAR_STRING`
/// column typing) the binary protocol use for every value this engine
/// produces. Callers handle `Value::Null` themselves — its wire
/// representation (`0xfb`) isn't a length-prefixed value at all.
fn encode_lenenc_value(payload: &mut Vec<u8>, val: &Value) {
    if val.is_null() {
        return;
    }
    // Found via testing before a public release: every value that wasn't
    // Text/Int/Float (DECIMAL, DATE, DATETIME, JSON, ...) used to be sent as
    // an *empty string*.
    let bytes: Vec<u8> = match val {
        Value::Bytes(b) => b.clone(),
        other => crate::mysql::exec::render_text(other).into_bytes(),
    };
    write_lenenc_int(payload, bytes.len() as u64);
    payload.extend_from_slice(&bytes);
}

/// Sends a `COM_STMT_EXECUTE` result set using the MySQL **binary**
/// protocol row format (distinct from `send_resultset`'s text protocol
/// format, which `COM_QUERY` uses). Every column is declared as
/// `VAR_STRING` regardless of its value's real type — the binary
/// protocol's per-column wire encoding is dictated by the declared column
/// type (e.g. `LONG` means 4 raw little-endian bytes, not a
/// length-encoded string), so claiming `VAR_STRING` lets every value use
/// the one lenenc-string encoder above, matching this engine's
/// "simple over performant" approach and how the text protocol path
/// already represents every value as text on the wire.
fn send_binary_resultset(
    stream: &mut TcpStream,
    mut seq: u8,
    rows: Vec<Vec<Value>>,
    affected_rows: u64,
    last_insert_id: u64,
    names: &[String],
) -> io::Result<()> {
    let Some(cols) = result_width(&rows, names) else {
        return send_ok(stream, seq, affected_rows, last_insert_id);
    };
    write_packet(stream, seq, &[cols as u8])?;
    seq = seq.wrapping_add(1);

    // Each column is declared with its real type and its values encoded in
    // that type's binary form. Found via testing before a public release:
    // every column used to be declared VAR_STRING, so a prepared statement
    // (mysql2's execute(), Go's database/sql, JDBC server-side prepares)
    // got every integer, DATETIME and JSON value back as a string.
    let mut types = Vec::with_capacity(cols);
    for i in 0..cols {
        let fallback = format!("col{i}");
        let name = names.get(i).filter(|n| n.as_str() != "?").unwrap_or(&fallback);
        let sample = column_sample(&rows, i);
        types.push(column_type_for(sample).0);
        let coldef = column_def_packet(name, sample);
        write_packet(stream, seq, &coldef)?;
        seq = seq.wrapping_add(1);
    }

    let st = STATUS.with(|s| s.get()).to_le_bytes();
    let eof = [0xfe, 0x00, 0x00, st[0], st[1]];
    write_packet(stream, seq, &eof)?;
    seq = seq.wrapping_add(1);

    // Binary Protocol Resultset Row: a 0x00 header byte, then a null
    // bitmap covering the columns offset by 2 bits (the first 2 bits are
    // reserved), then the non-NULL values in declared-type order.
    let bitmap_len = (cols + 2).div_ceil(8);
    for row in rows {
        let mut row_payload = vec![0x00];
        let mut bitmap = vec![0u8; bitmap_len];
        for (i, val) in row.iter().enumerate() {
            if val.is_null() {
                let bit_pos = i + 2;
                bitmap[bit_pos / 8] |= 1 << (bit_pos % 8);
            }
        }
        row_payload.extend_from_slice(&bitmap);
        for (i, val) in row.iter().enumerate() {
            if !val.is_null() {
                encode_binary_value(&mut row_payload, types[i], val);
            }
        }
        write_packet(stream, seq, &row_payload)?;
        seq = seq.wrapping_add(1);
    }

    write_packet(stream, seq, &eof)?;
    Ok(())
}

/// Decodes `COM_STMT_EXECUTE`'s parameter section (null-bitmap, optional
/// per-parameter type codes, then binary-protocol-encoded values) into the
/// `Value`s to substitute for `Expr::Param(0..num_params)`. `pos` is
/// advanced past whatever's consumed (unused after this call, but kept as
/// an out-param in case a caller wants to read more of the packet later).
fn decode_execute_params(
    payload: &[u8],
    pos: &mut usize,
    stmt_id: u32,
    num_params: usize,
    session: &mut Session,
) -> Result<Vec<Value>, MySqlError> {
    if num_params == 0 {
        return Ok(Vec::new());
    }

    let null_bitmap_len = num_params.div_ceil(8);
    if payload.len() < *pos + null_bitmap_len + 1 {
        return Err(MySqlError::syntax_error("malformed COM_STMT_EXECUTE packet"));
    }
    let null_bitmap = &payload[*pos..*pos + null_bitmap_len];
    let is_null = |i: usize| (null_bitmap[i / 8] >> (i % 8)) & 1 == 1;
    *pos += null_bitmap_len;

    let new_params_bound = payload[*pos];
    *pos += 1;

    let types: Vec<(u8, u8)> = if new_params_bound == 1 {
        let mut types = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            if *pos + 2 > payload.len() {
                return Err(MySqlError::syntax_error("malformed COM_STMT_EXECUTE packet"));
            }
            types.push((payload[*pos], payload[*pos + 1]));
            *pos += 2;
        }
        session.stmt_param_types.insert(stmt_id, types.clone());
        types
    } else {
        session.stmt_param_types.get(&stmt_id).cloned().ok_or_else(|| {
            MySqlError::unsupported(
                "COM_STMT_EXECUTE without parameter types and no cached types from a prior EXECUTE",
            )
        })?
    };

    let mut params = Vec::with_capacity(num_params);
    for (i, (ty, _flag)) in types.iter().enumerate().take(num_params) {
        if is_null(i) {
            params.push(Value::Null);
            continue;
        }
        let (val, consumed) = decode_binary_value(*ty, &payload[*pos..])?;
        params.push(val);
        *pos += consumed;
    }
    Ok(params)
}

/// Decodes one value from the MySQL binary protocol's per-parameter
/// encoding, given its `COM_STMT_EXECUTE` type code. Returns the value and
/// how many bytes it consumed: integers, floats, strings/blobs/decimals,
/// and the binary DATE/DATETIME/TIMESTAMP/TIME layouts.
fn decode_binary_value(ty: u8, buf: &[u8]) -> Result<(Value, usize), MySqlError> {
    let need = |n: usize| -> Result<(), MySqlError> {
        if buf.len() < n {
            Err(MySqlError::syntax_error("truncated bound parameter value"))
        } else {
            Ok(())
        }
    };
    match ty {
        0x01 => {
            need(1)?;
            Ok((Value::Int(buf[0] as i8 as i64), 1)) // MYSQL_TYPE_TINY
        }
        0x02 => {
            need(2)?;
            Ok((Value::Int(i16::from_le_bytes([buf[0], buf[1]]) as i64), 2)) // SHORT
        }
        0x03 | 0x09 => {
            need(4)?;
            Ok((Value::Int(i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as i64), 4)) // LONG, INT24
        }
        0x08 => {
            need(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[0..8]);
            Ok((Value::Int(i64::from_le_bytes(b)), 8)) // LONGLONG
        }
        0x04 => {
            need(4)?;
            let mut b = [0u8; 4];
            b.copy_from_slice(&buf[0..4]);
            Ok((Value::Float(f32::from_le_bytes(b) as f64), 4)) // FLOAT
        }
        0x05 => {
            need(8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[0..8]);
            Ok((Value::Float(f64::from_le_bytes(b)), 8)) // DOUBLE
        }
        // DECIMAL/NEWDECIMAL, and every text/blob/string type, are all
        // sent as a length-encoded string on the wire.
        0x00 | 0xf6 | 0xfc | 0xfd | 0xfe | 0x0f | 0xf9 | 0xfa | 0xfb => {
            let (s, n) = read_lenenc_string(buf)?;
            Ok((Value::Text(s), n))
        }
        0x06 => Ok((Value::Null, 0)), // MYSQL_TYPE_NULL (value should already be in the null-bitmap)
        0x0d => {
            need(2)?;
            Ok((Value::Int(u16::from_le_bytes([buf[0], buf[1]]) as i64), 2)) // YEAR
        }
        0xf5 | 0x10 => {
            let (s, n) = read_lenenc_string(buf)?; // JSON, BIT
            Ok((Value::Text(s), n))
        }
        // DATE / DATETIME / TIMESTAMP: a length byte (0, 4, 7 or 11), then
        // year (u16), month, day, [hour, minute, second], [microseconds
        // (u32)]. Found via testing before a public release: JDBC, Go and
        // mysql2 send every date value this way and it used to be refused.
        0x0a | 0x0c | 0x07 => {
            need(1)?;
            let len = buf[0] as usize;
            need(1 + len)?;
            let b = &buf[1..1 + len];
            if len == 0 {
                return Ok((Value::Text("0000-00-00 00:00:00".into()), 1));
            }
            let (y, mo, d) = (u16::from_le_bytes([b[0], b[1]]) as i64, b[2] as u32, b[3] as u32);
            let (h, mi, sec) =
                if len >= 7 { (b[4] as i64, b[5] as i64, b[6] as i64) } else { (0, 0, 0) };
            let us =
                if len >= 11 { u32::from_le_bytes([b[7], b[8], b[9], b[10]]) as i64 } else { 0 };
            if mo == 0 || d == 0 {
                return Ok((Value::Text(format!("{y:04}-{mo:02}-{d:02}")), 1 + len));
            }
            let days = crate::sql::datetime::date_from_ymd(y, mo, d);
            let v = if ty == 0x0a {
                Value::Date(days)
            } else {
                Value::Ts(
                    days as i64 * crate::sql::datetime::USECS_PER_DAY
                        + ((h * 60 + mi) * 60 + sec) * crate::sql::datetime::USECS_PER_SEC
                        + us,
                )
            };
            Ok((v, 1 + len))
        }
        // TIME: a length byte (0, 8 or 12), then negative flag, days (u32),
        // hour, minute, second, [microseconds (u32)].
        0x0b => {
            need(1)?;
            let len = buf[0] as usize;
            need(1 + len)?;
            let b = &buf[1..1 + len];
            if len == 0 {
                return Ok((Value::Time(0), 1));
            }
            let days = u32::from_le_bytes([b[1], b[2], b[3], b[4]]) as i64;
            let secs = ((days * 24 + b[5] as i64) * 60 + b[6] as i64) * 60 + b[7] as i64;
            let us =
                if len >= 12 { u32::from_le_bytes([b[8], b[9], b[10], b[11]]) as i64 } else { 0 };
            let total = secs * crate::sql::datetime::USECS_PER_SEC + us;
            Ok((Value::Time(if b[0] == 1 { -total } else { total }), 1 + len))
        }
        _ => Err(MySqlError::unsupported("bound parameter type")),
    }
}

fn read_lenenc_int(buf: &[u8]) -> Option<(u64, usize)> {
    let first = *buf.first()?;
    match first {
        0xfc => {
            if buf.len() < 3 {
                return None;
            }
            Some((u16::from_le_bytes([buf[1], buf[2]]) as u64, 3))
        }
        0xfd => {
            if buf.len() < 4 {
                return None;
            }
            Some((u32::from_le_bytes([buf[1], buf[2], buf[3], 0]) as u64, 4))
        }
        0xfe => {
            if buf.len() < 9 {
                return None;
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[1..9]);
            Some((u64::from_le_bytes(b), 9))
        }
        0xfb => Some((0, 1)), // NULL marker; not expected in this context
        v => Some((v as u64, 1)),
    }
}

fn read_lenenc_string(buf: &[u8]) -> Result<(String, usize), MySqlError> {
    let (len, hdr) = read_lenenc_int(buf)
        .ok_or_else(|| MySqlError::syntax_error("malformed length-encoded string"))?;
    let len = len as usize;
    if buf.len() < hdr + len {
        return Err(MySqlError::syntax_error("malformed length-encoded string"));
    }
    let s = String::from_utf8_lossy(&buf[hdr..hdr + len]).to_string();
    Ok((s, hdr + len))
}

/// Writes a length-encoded integer per the MySQL wire protocol.
fn write_lenenc_int(buf: &mut Vec<u8>, val: u64) {
    if val < 251 {
        buf.push(val as u8);
    } else if val < 0x1_0000 {
        buf.push(0xfc);
        buf.extend_from_slice(&(val as u16).to_le_bytes());
    } else if val < 0x1_0000_0000 {
        buf.push(0xfd);
        buf.extend_from_slice(&(val as u32).to_le_bytes()[0..3]);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&val.to_le_bytes());
    }
}

/// One non-NULL value in the binary result-row format for its column's
/// declared `ty` (see `column_type_for`).
fn encode_binary_value(out: &mut Vec<u8>, ty: u8, val: &Value) {
    use crate::sql::datetime::{USECS_PER_DAY, ymd_from_date};
    match (ty, val) {
        (0x08, Value::Int(i)) => out.extend_from_slice(&i.to_le_bytes()),
        (0x01, Value::Bool(b)) => out.push(u8::from(*b)),
        (0x05, Value::Float(f)) => out.extend_from_slice(&f.to_le_bytes()),
        (0x0a, Value::Date(d)) => {
            let (y, m, d) = ymd_from_date(*d);
            out.push(4);
            out.extend_from_slice(&(y as u16).to_le_bytes());
            out.extend_from_slice(&[m as u8, d as u8]);
        }
        (0x0c, Value::Ts(t)) => {
            let (y, mo, d) = ymd_from_date(t.div_euclid(USECS_PER_DAY) as i32);
            let us = t.rem_euclid(USECS_PER_DAY);
            let (h, mi, s, frac) =
                (us / 3_600_000_000, us / 60_000_000 % 60, us / 1_000_000 % 60, us % 1_000_000);
            out.push(if frac == 0 { 7 } else { 11 });
            out.extend_from_slice(&(y as u16).to_le_bytes());
            out.extend_from_slice(&[mo as u8, d as u8, h as u8, mi as u8, s as u8]);
            if frac != 0 {
                out.extend_from_slice(&(frac as u32).to_le_bytes());
            }
        }
        (0x0b, Value::Time(us)) => {
            let neg = *us < 0;
            let us = us.unsigned_abs();
            let (days, rest) = (us / 86_400_000_000, us % 86_400_000_000);
            let (h, mi, s, frac) = (
                rest / 3_600_000_000,
                rest / 60_000_000 % 60,
                rest / 1_000_000 % 60,
                rest % 1_000_000,
            );
            out.push(if frac == 0 { 8 } else { 12 });
            out.push(u8::from(neg));
            out.extend_from_slice(&(days as u32).to_le_bytes());
            out.extend_from_slice(&[h as u8, mi as u8, s as u8]);
            if frac != 0 {
                out.extend_from_slice(&(frac as u32).to_le_bytes());
            }
        }
        // DECIMAL, JSON, BLOB and strings: length-encoded bytes.
        _ => encode_lenenc_value(out, val),
    }
}
