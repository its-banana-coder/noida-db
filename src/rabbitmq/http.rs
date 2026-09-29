use crate::rabbitmq::engine::Engine;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

pub fn spawn(addr: &str, engine: Engine) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.clone();
            thread::spawn(move || {
                handle_http(stream, engine);
            });
        }
    });

    Ok(local_addr)
}

fn handle_http(mut stream: TcpStream, _engine: Engine) {
    let mut buffer = [0u8; 1024];
    if stream.read(&mut buffer).unwrap_or(0) == 0 {
        return;
    }

    let request = String::from_utf8_lossy(&buffer);
    let mut lines = request.lines();
    if let Some(req_line) = lines.next() {
        let parts: Vec<&str> = req_line.split_whitespace().collect();
        if parts.len() < 2 {
            return;
        }

        let path = parts[1];
        let response_body;

        if path.starts_with("/api/overview") {
            response_body =
                r#"{"management_version":"3.13.7", "rabbitmq_version":"3.13.7"}"#.to_string();
        } else if path.starts_with("/api/exchanges")
            || path.starts_with("/api/queues")
            || path.starts_with("/api/connections")
            || path.starts_with("/api/channels")
            || path.starts_with("/api/consumers")
            || path.starts_with("/api/users")
            || path.starts_with("/api/permissions")
        {
            response_body = r#"[]"#.to_string();
        } else if path.starts_with("/api/vhosts") {
            response_body = r#"[{"name": "/"}]"#.to_string();
        } else if path.starts_with("/api/whoami") {
            response_body = r#"{"name": "guest", "tags": "administrator"}"#.to_string();
        } else if path.starts_with("/api/nodes") {
            response_body = r#"[{"name": "rabbit@localhost"}]"#.to_string();
        } else if path.starts_with("/api/aliveness-test") || path.starts_with("/api/health") {
            response_body = r#"{"status":"ok"}"#.to_string();
        } else {
            response_body = r#"{"error":"not_found"}"#.to_string();
        }

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
    }
}
