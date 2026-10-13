//! The control API (see docs/control-api.md): a tiny HTTP/1.1 server on
//! 127.0.0.1 inside a running `noida-db start`, which `noida-db status`,
//! `stop <service>`, `down` and `ui` talk to. Found through
//! `<data_dir>/noida.control.json`, and every request must carry that
//! file's token. Hand-rolled on std, like the services' own protocols.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

/// Where a running server says how to reach it.
pub fn control_path(data_dir: &Path) -> PathBuf {
    data_dir.join("noida.control.json")
}

/// Starts the control API for the server whose data lives in `data_dir`,
/// and writes the control file (removed again on a clean shutdown). Also
/// turns the activity feed on.
pub fn serve(data_dir: &Path) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let token = random_token();
    let started_ms = crate::activity::now_ms();
    let file = json!({
        "pid": std::process::id(),
        "port": addr.port(),
        "token": token,
        "version": env!("CARGO_PKG_VERSION"),
        "started_ms": started_ms,
    });
    let path = control_path(data_dir);
    crate::persistence::write_snapshot_atomically(&path, file.to_string().as_bytes())?;
    let mine = file.to_string();
    crate::persistence::on_shutdown(move || {
        // Only our own file: a second server may have replaced it.
        if std::fs::read_to_string(&path).is_ok_and(|s| s == mine) {
            let _ = std::fs::remove_file(&path);
        }
    });
    crate::activity::enable();
    let data_dir = std::fs::canonicalize(data_dir).unwrap_or_else(|_| data_dir.to_path_buf());
    std::thread::Builder::new().name("control".into()).spawn(move || {
        for stream in listener.incoming().flatten() {
            let token = token.clone();
            let data_dir = data_dir.clone();
            let _ = std::thread::Builder::new().name("control-conn".into()).spawn(move || {
                let _ = handle(stream, &token, &data_dir, started_ms);
            });
        }
    })?;
    Ok(addr)
}

/// One request: read it, check the token, answer, close.
fn handle(stream: TcpStream, token: &str, data_dir: &Path, started_ms: u64) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.by_ref().take(8192).read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let mut authorized = false;
    let mut length = 0usize;
    for _ in 0..100 {
        line.clear();
        if reader.by_ref().take(8192).read_line(&mut line)? == 0 || line.trim().is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else { continue };
        let value = value.trim();
        if name.eq_ignore_ascii_case("authorization") {
            authorized = value.strip_prefix("Bearer ").is_some_and(|t| t.trim() == token);
        } else if name.eq_ignore_ascii_case("content-length") {
            length = value.parse().unwrap_or(0);
        }
    }
    // Bodies aren't used; read (a bounded amount) so the client isn't cut off.
    io::copy(&mut reader.take(length.min(1 << 20) as u64), &mut io::sink())?;

    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    let (status, body) = if !authorized {
        (401, error("missing or wrong token (see noida.control.json)"))
    } else {
        route(&method, path, query, data_dir, started_ms)
    };
    respond(stream, status, &body)?;
    if authorized && method == "POST" && path == "/v1/shutdown" {
        crate::persistence::request_shutdown();
    }
    Ok(())
}

fn route(method: &str, path: &str, query: &str, data_dir: &Path, started_ms: u64) -> (u16, Value) {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, segments.as_slice()) {
        ("GET", ["v1", "status"]) => {
            let services: Vec<Value> =
                crate::services::status().iter().map(|s| s.to_json()).collect();
            (
                200,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "pid": std::process::id(),
                    "data_dir": data_dir.display().to_string(),
                    "uptime_ms": crate::activity::now_ms().saturating_sub(started_ms),
                    "rss_bytes": rss_bytes(),
                    "services": services,
                }),
            )
        }
        ("POST", ["v1", "services", name, action]) => {
            let Some(name) = crate::config::service_name(name) else {
                return (404, error(&format!("unknown service '{name}'")));
            };
            let result = match *action {
                "start" => crate::services::start_service(name),
                "stop" => crate::services::stop_service(name),
                "restart" => crate::services::restart_service(name),
                "reset" => crate::services::reset_service(name),
                _ => return (404, error(&format!("unknown action '{action}'"))),
            };
            match result {
                (s, Ok(())) => (200, json!({ "service": s.to_json() })),
                (s, Err(msg)) => (409, json!({ "error": msg, "service": s.to_json() })),
            }
        }
        ("POST", ["v1", "shutdown"]) => (200, json!({ "ok": true })),
        ("GET", ["v1", "activity"]) => {
            let param =
                |key: &str| query.split('&').find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='));
            let since = param("since").and_then(|v| v.parse().ok()).unwrap_or(0);
            let limit = param("limit").and_then(|v| v.parse().ok()).unwrap_or(200);
            let (next, events) = crate::activity::read(since, limit);
            let events: Vec<Value> = events
                .iter()
                .map(|e| json!({"seq": e.seq, "ts_ms": e.ts_ms, "service": e.service, "text": e.text}))
                .collect();
            (200, json!({ "next": next, "events": events }))
        }
        (_, ["v1", "status" | "shutdown" | "activity"] | ["v1", "services", _, _]) => {
            (405, error(&format!("{method} not allowed on {path}")))
        }
        _ => (404, error(&format!("no such endpoint: {path}"))),
    }
}

fn error(msg: &str) -> Value {
    json!({ "error": msg })
}

fn respond(mut stream: TcpStream, status: u16, body: &Value) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        _ => "Error",
    };
    let body = body.to_string();
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// 32 hex characters from the OS's randomness (std seeds every
/// `RandomState` from it), without a dependency for it.
fn random_token() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut out = String::new();
    for i in 0..2u64 {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(i);
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out
}

/// This process's resident memory, where it's cheap to find out.
fn rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let kb = status.lines().find_map(|l| l.strip_prefix("VmRSS:"))?;
        kb.trim().trim_end_matches("kB").trim().parse::<u64>().ok().map(|kb| kb * 1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// A running server, found through its control file.
#[derive(Debug, Clone)]
pub struct Client {
    pub pid: u32,
    pub port: u16,
    token: String,
}

impl Client {
    /// The server running for `data_dir`, if one answers. A control file
    /// left behind by a server that died (or the port refusing) means none.
    pub fn find(data_dir: &Path) -> Option<Client> {
        let text = std::fs::read_to_string(control_path(data_dir)).ok()?;
        let file: Value = serde_json::from_str(&text).ok()?;
        let client = Client {
            pid: file["pid"].as_u64()? as u32,
            port: file["port"].as_u64()? as u16,
            token: file["token"].as_str()?.to_string(),
        };
        let (status, body) = client.request("GET", "/v1/status").ok()?;
        (status == 200 && body["pid"].as_u64() == Some(client.pid as u64)).then_some(client)
    }

    /// Sends one request and returns the status code and the JSON body.
    pub fn request(&self, method: &str, path: &str) -> io::Result<(u16, Value)> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        // Stopping a service waits for its connections; give it time.
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            self.token
        )?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let text = String::from_utf8_lossy(&response);
        let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad response"))?;
        Ok((status, serde_json::from_str(body).unwrap_or(Value::Null)))
    }

    /// Whether the server still answers at all (it closes its port only by
    /// exiting).
    pub fn alive(&self) -> bool {
        TcpStream::connect_timeout(
            &SocketAddr::from(([127, 0, 0, 1], self.port)),
            Duration::from_millis(500),
        )
        .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_random_hex() {
        let (a, b) = (random_token(), random_token());
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn routes_reject_unknown_paths_and_methods() {
        let dir = Path::new("/nonexistent");
        assert_eq!(route("GET", "/v1/nope", "", dir, 0).0, 404);
        assert_eq!(route("DELETE", "/v1/status", "", dir, 0).0, 405);
        assert_eq!(route("POST", "/v1/services/oracle/start", "", dir, 0).0, 404);
        assert_eq!(route("POST", "/v1/services/redis/explode", "", dir, 0).0, 404);
        let (status, body) = route("GET", "/v1/activity", "since=0&limit=5", dir, 0);
        assert_eq!(status, 200);
        assert!(body["next"].is_u64());
        assert!(body["events"].is_array());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rss_is_known_on_linux() {
        assert!(rss_bytes().unwrap() > 0);
    }
}
