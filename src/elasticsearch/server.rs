use super::engine::{Engine, error};
use serde_json::Value;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::thread;
use std::time::Duration;

/// Like `spawn_persistent`, but also returns a closure that performs
/// exactly the save `spawn_persistent`'s own shutdown hook would perform,
/// so a test can trigger a save directly instead of going through
/// `persistence::on_shutdown`'s process-wide hook registry -- that
/// registry runs *every* hook ever registered in the process, which is
/// unsafe to trigger from a single test once more than one persistent
/// server has been started in the same test binary (as will happen once
/// other services' persistence tests exist alongside this one).
pub fn spawn_persistent_for_test(
    addr: &str,
    data_dir: &Path,
) -> io::Result<(SocketAddr, impl Fn() + Send + Sync + 'static)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let snapshot_path = data_dir.join("elasticsearch.json");

    let engine = if let Ok(bytes) = std::fs::read(&snapshot_path) {
        Engine::load(&bytes)
    } else {
        Engine::default()
    };

    let save_engine = engine.clone();
    let save = move || {
        let snapshot = save_engine.snapshot();
        let _ = crate::persistence::write_snapshot_atomically(&snapshot_path, &snapshot);
    };

    thread::Builder::new().name("elasticsearch-listener".into()).spawn(move || {
        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    let engine = engine.clone();
                    let _ = thread::Builder::new().name("elasticsearch-client".into()).spawn(
                        move || {
                            let _ = serve(stream, engine);
                        },
                    );
                }
                Err(e) => eprintln!("elasticsearch accept: {e}"),
            }
        }
    })?;
    Ok((local, save))
}

pub fn spawn_persistent(addr: &str, data_dir: &Path) -> io::Result<SocketAddr> {
    let (addr, save) = spawn_persistent_for_test(addr, data_dir)?;
    crate::persistence::on_shutdown(save);
    Ok(addr)
}

pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let engine = Engine::default();
    thread::Builder::new().name("elasticsearch-listener".into()).spawn(move || {
        for incoming in listener.incoming() {
            match incoming {
                Ok(stream) => {
                    let engine = engine.clone();
                    let _ = thread::Builder::new().name("elasticsearch-client".into()).spawn(
                        move || {
                            let _ = serve(stream, engine);
                        },
                    );
                }
                Err(e) => eprintln!("elasticsearch accept: {e}"),
            }
        }
    })?;
    Ok(local)
}

fn serve(stream: TcpStream, engine: Engine) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let mut reader = BufReader::new(stream);
    loop {
        let mut first = String::new();
        if reader.read_line(&mut first)? == 0 {
            break;
        }
        if first == "\r\n" {
            continue;
        }
        let mut p = first.split_whitespace();
        let method = p.next().unwrap_or("").to_ascii_uppercase();
        let target = p.next().unwrap_or("/").to_string();
        let mut headers = std::collections::HashMap::new();
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                return Ok(());
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
        let body = if headers
            .get("transfer-encoding")
            .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
        {
            read_chunked(&mut reader)?
        } else {
            let len =
                headers.get("content-length").and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
            let mut body = vec![0; len];
            reader.read_exact(&mut body)?;
            body
        };
        let (path, query) = target.split_once('?').unwrap_or((&target, ""));
        let (status, payload) = if method == "OPTIONS" {
            (200, serde_json::json!({}))
        } else {
            engine.dispatch(&method, path, query, &body)
        };
        let head = method == "HEAD";
        let (status, payload) =
            if head && status == 200 { (200, Value::Null) } else { (status, payload) };
        let bytes = if head {
            Vec::new()
        } else {
            serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec())
        };
        let reason = match status {
            200 => "OK",
            201 => "Created",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            409 => "Conflict",
            _ => "Internal Server Error",
        };
        let connection_close =
            headers.get("connection").map(|v| v.eq_ignore_ascii_case("close")).unwrap_or(false);
        let w = reader.get_mut();
        write!(
            w,
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: {}\r\nX-Elastic-Product: Elasticsearch\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, PUT, DELETE, HEAD, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Elastic-Client-Meta\r\n{}\r\n",
            bytes.len(),
            if connection_close { "Connection: close\r\n" } else { "" }
        )?;
        w.write_all(&bytes)?;
        w.flush()?;
        if connection_close
            || headers.get("connection").is_some_and(|v| !v.eq_ignore_ascii_case("keep-alive"))
        {
            break;
        }
    }
    Ok(())
}

fn read_chunked<R: BufRead>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or("0"), 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk size"))?;
        if size == 0 {
            let mut trailer = String::new();
            loop {
                trailer.clear();
                if reader.read_line(&mut trailer)? == 0 || trailer == "\r\n" || trailer == "\n" {
                    break;
                }
            }
            break;
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..])?;
        let mut crlf = [0; 2];
        reader.read_exact(&mut crlf)?;
    }
    Ok(body)
}

#[allow(dead_code)]
fn _error_type_anchor(_: Value) {
    let _ = error("x", "x", 500);
}
