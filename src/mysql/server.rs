use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use crate::mysql::engine::Engine;
use crate::mysql::types::Value;

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
        if stream.read_exact(&mut header).is_err() { break; }
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let seq = header[3];
        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).is_err() { break; }

        if len == 0 { continue; }

        match payload[0] {
            0x01 => break, // Quit
            0x02 => { // Init DB
                let db = String::from_utf8_lossy(&payload[1..]);
                engine.use_db(&db);
                send_ok(&mut stream, seq.wrapping_add(1))?;
            }
            0x03 => { // Query
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

fn send_ok(stream: &mut TcpStream, seq: u8) -> io::Result<()> {
    let ok = b"\x00\x00\x00\x02\x00\x00\x00";
    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(ok.len() as u32).to_le_bytes()[0..3]);
    header[3] = seq;
    stream.write_all(&header)?;
    stream.write_all(ok)?;
    Ok(())
}

fn send_resultset(stream: &mut TcpStream, mut seq: u8, rows: Vec<Vec<Value>>) -> io::Result<()> {
    if rows.is_empty() {
        return send_ok(stream, seq);
    }

    // Column count
    let cols = rows[0].len();
    let mut header = [0u8; 4];
    header[0] = cols as u8;
    header[3] = seq;
    stream.write_all(&header)?;
    stream.write_all(&[cols as u8])?;
    seq = seq.wrapping_add(1);

    for i in 0..cols {
        let coldef = b"\x03def\x00\x00\x00\x04col1\x00\x0c\x3f\x00\x0a\x00\x00\x00\x00\x00\x00\x00";
        let mut header = [0u8; 4];
        header[0..3].copy_from_slice(&(coldef.len() as u32).to_le_bytes()[0..3]);
        header[3] = seq;
        stream.write_all(&header)?;
        stream.write_all(coldef)?;
        seq = seq.wrapping_add(1);
    }

    let eof = b"\xfe\x00\x00\x02\x00";
    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(eof.len() as u32).to_le_bytes()[0..3]);
    header[3] = seq;
    stream.write_all(&header)?;
    stream.write_all(eof)?;
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
                Value::Null => {
                    row_payload.push(0xfb);
                }
                _ => {
                    row_payload.push(0);
                }
            }
        }
        let mut header = [0u8; 4];
        header[0..3].copy_from_slice(&(row_payload.len() as u32).to_le_bytes()[0..3]);
        header[3] = seq;
        stream.write_all(&header)?;
        stream.write_all(&row_payload)?;
        seq = seq.wrapping_add(1);
    }

    let mut header = [0u8; 4];
    header[0..3].copy_from_slice(&(eof.len() as u32).to_le_bytes()[0..3]);
    header[3] = seq;
    stream.write_all(&header)?;
    stream.write_all(eof)?;

    Ok(())
}
