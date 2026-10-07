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
    crate::persistence::on_save("elasticsearch", save);
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
    // See the identical note in src/mysql/server.rs: disabling Nagle's
    // algorithm here avoids the same class of request-latency stall for
    // any response written in more than one syscall.
    stream.set_nodelay(true)?;
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
        // `Content-Encoding: gzip` (elasticsearch-py's `http_compress`,
        // the Java client's compression option).
        let body = if headers
            .get("content-encoding")
            .is_some_and(|v| v.eq_ignore_ascii_case("gzip"))
        {
            let mut out = Vec::new();
            match std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&body[..]), &mut out)
            {
                Ok(_) => out,
                Err(_) => Vec::from(&b"\x00not gzip"[..]),
            }
        } else {
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
        let raw_text =
            payload.get(super::cat::RAW_TEXT).and_then(Value::as_str).map(str::to_string);
        let bytes = if head {
            Vec::new()
        } else if let Some(text) = &raw_text {
            text.as_bytes().to_vec()
        } else {
            to_es_json(&payload)
        };
        let content_type = if raw_text.is_some() {
            "text/plain; charset=UTF-8"
        } else {
            "application/json; charset=UTF-8"
        };
        let reason = match status {
            200 => "OK",
            201 => "Created",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            409 => "Conflict",
            _ => "Internal Server Error",
        };
        let connection_close =
            headers.get("connection").map(|v| v.eq_ignore_ascii_case("close")).unwrap_or(false);
        // Compressed responses for a client that asks, as Elasticsearch
        // does (`http.compression` is on by default).
        let gzip_out = !bytes.is_empty()
            && headers
                .get("accept-encoding")
                .is_some_and(|v| v.split(',').any(|e| e.trim().split(';').next() == Some("gzip")));
        let bytes = if gzip_out {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(&bytes)?;
            enc.finish()?
        } else {
            bytes
        };
        let w = reader.get_mut();
        write!(
            w,
            "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nX-Elastic-Product: Elasticsearch\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, PUT, DELETE, HEAD, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Elastic-Client-Meta\r\n{}{}\r\n",
            bytes.len(),
            if gzip_out { "Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n" } else { "" },
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

/// Elasticsearch's key order for the keys whose position clients and
/// tools see (an error's `type` before its `reason`, a document's
/// metadata before `_source`); every other key keeps serde's order.
const KEY_ORDER: &[&str] = &[
    "error",
    "root_cause",
    "type",
    "reason",
    "_index",
    "_id",
    "_version",
    "_seq_no",
    "_primary_term",
    "_routing",
    "found",
    "result",
    "_shards",
    "_score",
    "_source",
];

fn to_es_json(v: &Value) -> Vec<u8> {
    fn write(v: &Value, out: &mut Vec<u8>) {
        match v {
            Value::Object(m) => {
                let rank = |k: &str| KEY_ORDER.iter().position(|x| *x == k).unwrap_or(usize::MAX);
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort_by_key(|k| rank(k));
                out.push(b'{');
                for (n, k) in keys.into_iter().enumerate() {
                    if n > 0 {
                        out.push(b',');
                    }
                    out.extend(serde_json::to_vec(k).unwrap_or_default());
                    out.push(b':');
                    write(&m[k], out);
                }
                out.push(b'}');
            }
            Value::Array(a) => {
                out.push(b'[');
                for (n, x) in a.iter().enumerate() {
                    if n > 0 {
                        out.push(b',');
                    }
                    write(x, out);
                }
                out.push(b']');
            }
            other => out.extend(serde_json::to_vec(other).unwrap_or_default()),
        }
    }
    let mut out = Vec::new();
    write(v, &mut out);
    out
}
