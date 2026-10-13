//! A fake control server implementing the contract with canned state (tests only).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

pub struct FakeState {
    pub running: Vec<(String, bool)>,
    pub requests: Vec<String>,
    pub events: Vec<(u64, String, String)>,
    pub shutdown: bool,
}

pub struct Fake {
    pub port: u16,
    pub token: String,
    pub state: Arc<Mutex<FakeState>>,
}

impl Fake {
    pub fn start() -> Fake {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let token = "tok123".to_string();
        let state = Arc::new(Mutex::new(FakeState {
            running: vec![
                ("postgres".into(), true),
                ("redis".into(), false),
                ("clickhouse".into(), false),
            ],
            requests: vec![],
            events: vec![
                (1, "postgres".into(), "CREATE TABLE users (id int)".into()),
                (2, "redis".into(), "SET a 1".into()),
                (3, "postgres".into(), "SELECT 1".into()),
            ],
            shutdown: false,
        }));
        let (st, tk) = (state.clone(), token.clone());
        std::thread::spawn(move || {
            for conn in l.incoming() {
                let Ok(mut c) = conn else { break };
                let mut buf = [0u8; 4096];
                let n = c.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let first = req.lines().next().unwrap_or("").to_string();
                let mut it = first.split_whitespace();
                let (method, path) = (it.next().unwrap_or(""), it.next().unwrap_or("").to_string());
                let (code, body) = if !req.contains(&format!("Bearer {tk}")) {
                    (401, r#"{"error":"unauthorized"}"#.to_string())
                } else {
                    handle(&mut st.lock().unwrap(), method, &path)
                };
                let resp = format!(
                    "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = c.write_all(resp.as_bytes());
            }
        });
        Fake { port, token, state }
    }

    pub fn client(&self) -> super::api::Client {
        super::api::Client { port: self.port, token: self.token.clone() }
    }

    pub fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }
}

fn svc_json(name: &str, on: bool) -> String {
    let port = match name {
        "postgres" => 5432,
        "redis" => 6379,
        _ => 8123,
    };
    let connect = match name {
        "postgres" => format!("postgres://postgres@127.0.0.1:{port}/postgres"),
        "redis" => format!("redis://127.0.0.1:{port}"),
        _ => format!("http://127.0.0.1:{port}"),
    };
    let state = if name == "clickhouse" {
        "unavailable"
    } else if on {
        "running"
    } else {
        "stopped"
    };
    let bytes = if on { "6291456" } else { "null" };
    format!(
        r#"{{"name":"{name}","state":"{state}","host":"127.0.0.1","port":{port},"connect":"{connect}","data_bytes":{bytes},"error":null}}"#
    )
}

fn handle(s: &mut FakeState, method: &str, path: &str) -> (u16, String) {
    s.requests.push(format!("{method} {path}"));
    let parts: Vec<&str> = path.split('?').next().unwrap().trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        ("GET", ["v1", "status"]) => {
            let svcs: Vec<String> = s.running.iter().map(|(n, on)| svc_json(n, *on)).collect();
            (
                200,
                format!(
                    r#"{{"version":"0.2.3","pid":1,"data_dir":"/d","uptime_ms":1000,"rss_bytes":7340032,"services":[{}]}}"#,
                    svcs.join(",")
                ),
            )
        }
        ("GET", ["v1", "activity"]) => {
            let since: u64 = path
                .split("since=")
                .nth(1)
                .and_then(|r| r.split('&').next())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let evs: Vec<String> = s
                .events
                .iter()
                .filter(|e| e.0 > since)
                .map(|(q, sv, t)| {
                    format!(
                        r#"{{"seq":{q},"ts_ms":{},"service":"{sv}","text":"{t}"}}"#,
                        43_263_000 + q * 1000
                    )
                })
                .collect();
            let next = s.events.last().map(|e| e.0).unwrap_or(since).max(since);
            (200, format!(r#"{{"next":{next},"events":[{}]}}"#, evs.join(",")))
        }
        ("POST", ["v1", "services", name, verb]) => {
            let Some(e) = s.running.iter_mut().find(|(n, _)| n == name) else {
                return (404, r#"{"error":"unknown service"}"#.into());
            };
            match *verb {
                "start" => e.1 = true,
                "stop" => e.1 = false,
                _ => {}
            }
            (200, format!(r#"{{"service":{}}}"#, svc_json(name, e.1)))
        }
        ("POST", ["v1", "shutdown"]) => {
            s.shutdown = true;
            (200, r#"{"ok":true}"#.into())
        }
        _ => (404, r#"{"error":"not found"}"#.into()),
    }
}
