use crate::mysql::binder::Binder;
use crate::mysql::engine::Engine;
use crate::mysql::plan::Plan;
use crate::mysql::types::Value;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let _ = thread::spawn(move || {
                let _ = serve(stream);
            });
        }
    });
    Ok(local)
}

struct Session {
    engine: Engine,
    stmt_id_counter: u32,
    prepared_stmts: HashMap<u32, (String, Plan, u16)>, // ID -> (sql, Plan, num_params)
}

fn serve(mut stream: TcpStream) -> io::Result<()> {
    let mut session =
        Session { engine: Engine::new(), stmt_id_counter: 1, prepared_stmts: HashMap::new() };

    // Send Handshake
    let handshake = b"\x0a\x35\x2e\x35\x2e\x35\x2d\x31\x30\x2e\x34\x2e\x32\x32\x2d\x4d\x61\x72\x69\x61\x44\x42\x00\x01\x00\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x00\xff\xff\x21\x02\x00\x0f\xc0\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x39\x30\x31\x32\x00\x6d\x79\x73\x71\x6c\x5f\x6e\x61\x74\x69\x76\x65\x5f\x70\x61\x73\x73\x77\x6f\x72\x64\x00";
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
                let _ = send_ok(&mut stream, seq.wrapping_add(1));
            }
            0x03 => {
                // Query
                let q = String::from_utf8_lossy(&payload[1..]);
                match session.engine.execute(&q) {
                    Ok(rows) => {
                        let _ = send_resultset(&mut stream, seq.wrapping_add(1), rows);
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
                        let mut binder = Binder::new(session.engine.current_db.clone());
                        match binder.bind_statement(stmt) {
                            Ok(plan) => {
                                let stmt_id = session.stmt_id_counter;
                                session.stmt_id_counter += 1;

                                let num_params = count_params(&plan) as u16;
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
                                let _ = write_packet(&mut stream, seq.wrapping_add(1), &prep_ok);
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
                // Stmt Execute
                if payload.len() < 5 {
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

                if let Some((_sql, plan, _num_params)) = session.prepared_stmts.get(&stmt_id) {
                    // For now, assume no parameters and execute the plan directly.
                    // Parameter binding logic would require extracting the null bitmap
                    // and types from the payload, then updating the plan params.
                    let mut executor = crate::mysql::exec::Executor::new(
                        session.engine.db.clone(),
                        session.engine.current_db.clone(),
                    );
                    match executor.execute_plan(plan.clone()) {
                        Ok(rows) => {
                            let _ = send_resultset(&mut stream, seq.wrapping_add(1), rows);
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
                } else {
                    let _ = send_err(
                        &mut stream,
                        seq.wrapping_add(1),
                        1243,
                        "HY000",
                        "Unknown prepared statement handler",
                    );
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
                let _ = send_ok(&mut stream, seq.wrapping_add(1));
            }
            _ => {
                let _ = send_ok(&mut stream, seq.wrapping_add(1));
            }
        }
    }

    Ok(())
}

fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> io::Result<()> {
    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(payload.len() as u32).to_le_bytes()[0..3]);
    header[3] = seq;
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    Ok(())
}

fn send_ok(stream: &mut TcpStream, seq: u8) -> io::Result<()> {
    write_packet(stream, seq, b"\x00\x00\x00\x02\x00\x00\x00")
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

fn column_type_for(val: Option<&Value>) -> (u8, u16) {
    match val {
        Some(Value::Int(_)) => (0x03, 0x3f), // MYSQL_TYPE_LONG, binary charset
        Some(Value::Float(_)) => (0x05, 0x3f), // MYSQL_TYPE_DOUBLE, binary charset
        _ => (0xfd, 0x2d),                   // MYSQL_TYPE_VAR_STRING, utf8mb4_general_ci
    }
}

fn column_def_packet(name: &str, val: Option<&Value>) -> Vec<u8> {
    let (col_type, charset) = column_type_for(val);
    let mut p = Vec::new();
    p.push(3);
    p.extend_from_slice(b"def"); // catalog
    p.push(0); // schema
    p.push(0); // table
    p.push(0); // org_table
    p.push(name.len() as u8);
    p.extend_from_slice(name.as_bytes()); // name
    p.push(0); // org_name
    p.push(0x0c); // length of fixed-length fields below (always 12)
    p.extend_from_slice(&charset.to_le_bytes()); // character_set (2)
    p.extend_from_slice(&255u32.to_le_bytes()); // column_length (4)
    p.push(col_type); // type (1)
    p.extend_from_slice(&0u16.to_le_bytes()); // flags (2)
    p.push(0); // decimals (1)
    p.extend_from_slice(&[0, 0]); // filler (2, reserved)
    p
}

fn send_resultset(stream: &mut TcpStream, mut seq: u8, rows: Vec<Vec<Value>>) -> io::Result<()> {
    if rows.is_empty() {
        return send_ok(stream, seq);
    }

    let cols = rows[0].len();
    write_packet(stream, seq, &[cols as u8])?;
    seq = seq.wrapping_add(1);

    for i in 0..cols {
        let name = format!("col{i}");
        let sample = rows[0].get(i);
        let coldef = column_def_packet(&name, sample);
        write_packet(stream, seq, &coldef)?;
        seq = seq.wrapping_add(1);
    }

    let eof = b"\xfe\x00\x00\x02\x00";
    write_packet(stream, seq, eof)?;
    seq = seq.wrapping_add(1);

    for row in rows {
        let mut row_payload = Vec::new();
        for val in row {
            match val {
                Value::Text(s) => {
                    row_payload.push(s.len() as u8);
                    row_payload.extend_from_slice(s.as_bytes());
                }
                Value::Int(i) => {
                    let s = i.to_string();
                    row_payload.push(s.len() as u8);
                    row_payload.extend_from_slice(s.as_bytes());
                }
                Value::Float(f) => {
                    let s = f.to_string();
                    row_payload.push(s.len() as u8);
                    row_payload.extend_from_slice(s.as_bytes());
                }
                Value::Null => {
                    row_payload.push(0xfb);
                }
                _ => {
                    row_payload.push(0);
                }
            }
        }
        write_packet(stream, seq, &row_payload)?;
        seq = seq.wrapping_add(1);
    }

    write_packet(stream, seq, eof)?;

    Ok(())
}

fn count_params(_plan: &Plan) -> usize {
    // Basic stub for parameters count. Ideally, would traverse the `Plan` AST.
    0
}
