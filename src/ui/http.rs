//! A tiny std-only HTTP/1.1 client for the loopback control API.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

#[derive(Debug, PartialEq)]
pub enum ApiError {
    /// Could not reach the server (refused, timed out, garbled reply).
    Down(String),
    /// The server answered with a 4xx/5xx status.
    Http(u16, String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Down(m) => write!(f, "{m}"),
            ApiError::Http(c, m) => write!(f, "{m} (HTTP {c})"),
        }
    }
}

/// Send one request with a Bearer token; returns the parsed JSON body of a 2xx reply.
pub fn request(port: u16, token: &str, method: &str, path: &str) -> Result<Value, ApiError> {
    let down = |e: std::io::Error| ApiError::Down(e.to_string());
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).map_err(down)?;
    s.set_read_timeout(Some(Duration::from_secs(5))).map_err(down)?;
    s.set_write_timeout(Some(Duration::from_secs(2))).map_err(down)?;
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\n\
         Accept: application/json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).map_err(down)?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).map_err(down)?;
    parse_response(&raw)
}

pub fn parse_response(raw: &[u8]) -> Result<Value, ApiError> {
    let bad = |m: &str| ApiError::Down(m.to_string());
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| bad("truncated HTTP response"))?;
    let head = String::from_utf8_lossy(&raw[..split]);
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| bad("bad HTTP status line"))?;
    let chunked = lines.any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let body = &raw[split + 4..];
    let body = if chunked { dechunk(body) } else { body.to_vec() };
    let json: Value = if body.iter().all(u8::is_ascii_whitespace) {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|e| bad(&format!("bad JSON body: {e}")))?
    };
    if status >= 400 {
        let msg = json.get("error").and_then(Value::as_str).unwrap_or("request failed");
        return Err(ApiError::Http(status, msg.to_string()));
    }
    Ok(json)
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(nl) = b.windows(2).position(|w| w == b"\r\n") {
        let size = std::str::from_utf8(&b[..nl])
            .ok()
            .and_then(|s| usize::from_str_radix(s.split(';').next().unwrap_or("").trim(), 16).ok())
            .unwrap_or(0);
        if size == 0 || b.len() < nl + 2 + size {
            break;
        }
        out.extend_from_slice(&b[nl + 2..nl + 2 + size]);
        b = b.get(nl + 2 + size + 2..).unwrap_or(&[]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ok_and_error() {
        let ok = b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}";
        assert_eq!(parse_response(ok).unwrap()["ok"], true);
        let err = b"HTTP/1.1 404 Not Found\r\n\r\n{\"error\":\"no such service\"}";
        assert_eq!(parse_response(err), Err(ApiError::Http(404, "no such service".into())));
        assert!(matches!(parse_response(b"garbage"), Err(ApiError::Down(_))));
    }

    #[test]
    fn parses_chunked() {
        let r = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n{\"a\":\r\n3\r\n1}\n\r\n0\r\n\r\n";
        assert_eq!(parse_response(r).unwrap()["a"], 1);
    }
}
