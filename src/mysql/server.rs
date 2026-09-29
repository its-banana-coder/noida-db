use crate::mysql::engine::Engine;
use crate::mysql::types::Value;
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

fn serve(mut stream: TcpStream) -> io::Result<()> {
    let mut engine = Engine::new();

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
    // In a full implementation we would check the hash.
    // For local dev, we accept.

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
                engine.use_db(&db);
                send_ok(&mut stream, seq.wrapping_add(1))?;
            }
            0x03 => {
                // Query
                let q = String::from_utf8_lossy(&payload[1..]);
                match engine.execute(&q) {
                    Ok(rows) => {
                        send_resultset(&mut stream, seq.wrapping_add(1), rows)?;
                    }
                    Err(_e) => {
                        // Error handling placeholder
                        send_ok(&mut stream, seq.wrapping_add(1))?;
                    }
                }
            }
            _ => {
                send_ok(&mut stream, seq.wrapping_add(1))?;
            }
        }
    }

    Ok(())
}

/// Writes one MySQL protocol packet: a 3-byte little-endian length prefix
/// followed by a 1-byte sequence number, then `payload`. Every packet in
/// this module must go through this so the header's length always matches
/// what's actually sent — a hand-rolled header/payload pair got this wrong
/// once already (the column-count packet used to put the column count
/// itself into the length byte) and a malformed column-definition packet
/// (missing its trailing filler bytes) desynced real clients badly enough
/// to hang forever waiting for bytes that would never arrive as expected.
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

/// A MySQL type code and charset for a column, guessed from the first row's
/// value at that index (this engine doesn't track a real schema/column
/// names yet, so `colN` and a type inferred from the sole sample row is
/// the best available placeholder).
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
    // Length-encoded integer: a single byte IS the value for cols < 251,
    // which covers every realistic result set.
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
