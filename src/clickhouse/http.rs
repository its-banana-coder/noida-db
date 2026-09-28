//! A minimal HTTP/1.1 codec: just enough for ClickHouse's HTTP interface
//! (`GET`/`POST /` with a `query` param and/or body, `GET /ping`),
//! including `Transfer-Encoding: chunked` request bodies (streaming
//! `INSERT`s, e.g. from the official Rust client, use it instead of
//! `Content-Length`). No keep-alive on the response side: every response
//! closes the connection, same as noida-db does for real clients that
//! always reconnect cleanly.

use std::collections::HashMap;
use std::io::{self, BufRead, Write};

pub struct Request {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub body: Vec<u8>,
}

/// Reads one request from `reader`. `Ok(None)` means the client closed the
/// connection before sending anything (or sent a blank line first).
pub fn read_request(reader: &mut impl BufRead) -> io::Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let line = line.trim_end();
    if line.is_empty() {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/");
    let (path, query) = split_target(target);

    let mut content_length = 0usize;
    let mut chunked = false;
    loop {
        let mut hline = String::new();
        if reader.read_line(&mut hline)? == 0 {
            break;
        }
        let hline = hline.trim_end();
        if hline.is_empty() {
            break;
        }
        if let Some((k, v)) = hline.split_once(':') {
            let k = k.trim();
            let v = v.trim();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            } else if k.eq_ignore_ascii_case("transfer-encoding")
                && v.eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            }
        }
    }

    let body = if chunked {
        read_chunked_body(reader)?
    } else {
        let mut body = vec![0u8; content_length];
        if content_length > 0 {
            reader.read_exact(&mut body)?;
        }
        body
    };

    Ok(Some(Request { method, path, query, body }))
}

/// Reads a `Transfer-Encoding: chunked` body: a size line (hex, ignoring any
/// `;extension`), that many bytes, a trailing CRLF, repeated until a
/// zero-size chunk: `0\r\n\r\n` (trailer headers, if any, are discarded).
fn read_chunked_body(reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        reader.read_line(&mut size_line)?;
        let size_line = size_line.trim_end();
        let size_str = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if size == 0 {
            // Trailer headers (rare, and none of ClickHouse's clients send
            // any), up to the final blank line.
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line)? == 0 || line.trim_end().is_empty() {
                    break;
                }
            }
            break;
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk)?;
        body.extend_from_slice(&chunk);
        // Each chunk's data is followed by a CRLF that isn't part of it.
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf)?;
    }
    Ok(body)
}

fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, qs) = target.split_once('?').unwrap_or((target, ""));
    let mut query = HashMap::new();
    for pair in qs.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        query.insert(url_decode(k), url_decode(v));
    }
    (path.to_string(), query)
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn write(&self, w: &mut impl Write) -> io::Result<()> {
        write!(w, "HTTP/1.1 {} {}\r\n", self.status, reason_phrase(self.status))?;
        for (k, v) in &self.headers {
            write!(w, "{k}: {v}\r\n")?;
        }
        write!(w, "Content-Length: {}\r\n", self.body.len())?;
        write!(w, "Connection: close\r\n\r\n")?;
        w.write_all(&self.body)
    }
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Internal Server Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parses_get_with_query_string() {
        let raw = "GET /?query=SELECT%201&default_format=JSON HTTP/1.1\r\nHost: x\r\n\r\n";
        let req = read_request(&mut Cursor::new(raw)).unwrap().unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/");
        assert_eq!(req.query.get("query"), Some(&"SELECT 1".to_string()));
        assert_eq!(req.query.get("default_format"), Some(&"JSON".to_string()));
    }

    #[test]
    fn parses_post_with_body() {
        let body = "SELECT 1";
        let raw =
            format!("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
        let req = read_request(&mut Cursor::new(raw)).unwrap().unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.body, body.as_bytes());
    }

    #[test]
    fn parses_chunked_body() {
        let raw = "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
                    5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let req = read_request(&mut Cursor::new(raw)).unwrap().unwrap();
        assert_eq!(req.body, b"hello world");
    }

    #[test]
    fn chunked_body_with_trailer_headers() {
        let raw = "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
                    3\r\nabc\r\n0\r\nX-Trailer: ignored\r\n\r\n";
        let req = read_request(&mut Cursor::new(raw)).unwrap().unwrap();
        assert_eq!(req.body, b"abc");
    }

    #[test]
    fn closed_connection_is_none() {
        let req = read_request(&mut Cursor::new("")).unwrap();
        assert!(req.is_none());
    }

    #[test]
    fn response_write_includes_status_and_headers() {
        let resp = Response {
            status: 200,
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: b"Ok.\n".to_vec(),
        };
        let mut out = Vec::new();
        resp.write(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("Content-Type: text/plain\r\n"));
        assert!(text.ends_with("Ok.\n"));
    }
}
