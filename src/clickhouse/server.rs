//! HTTP front end: one thread per connection, one query per request.

use std::io::BufReader;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use super::engine;
use super::error::ChError;
use super::format;
use super::http::{self, Request, Response};

/// Connection threads touch little stack; keep reservations small.
const STACK_SIZE: usize = 256 * 1024;

static QUERY_ID: AtomicU64 = AtomicU64::new(1);

/// Binds `addr` and serves ClickHouse's HTTP interface on background
/// threads.
pub fn spawn(addr: &str) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    thread::Builder::new().name("clickhouse-accept".into()).stack_size(STACK_SIZE).spawn(
        move || {
            for stream in listener.incoming().flatten() {
                let _ = thread::Builder::new()
                    .name("clickhouse-conn".into())
                    .stack_size(STACK_SIZE)
                    .spawn(move || handle(stream));
            }
        },
    )?;
    Ok(local)
}

fn handle(stream: TcpStream) {
    let mut reader = BufReader::new(match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    });
    let mut writer = stream;
    if let Ok(Some(req)) = http::read_request(&mut reader) {
        let _ = route(&req).write(&mut writer);
    }
}

fn route(req: &Request) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/ping") => text_ok("Ok.\n"),
        ("GET", "/replicas_status") => text_ok("Ok.\n"),
        (_, "/") => run_query(req),
        _ => Response { status: 404, headers: vec![], body: b"Not Found\n".to_vec() },
    }
}

fn text_ok(body: &str) -> Response {
    Response {
        status: 200,
        headers: vec![("Content-Type".into(), "text/plain; charset=UTF-8".into())],
        body: body.as_bytes().to_vec(),
    }
}

fn run_query(req: &Request) -> Response {
    let Some(query) = query_text(req) else {
        return error_response(&ChError::syntax("no query"));
    };
    let query_id = req
        .query
        .get("query_id")
        .cloned()
        .unwrap_or_else(|| format!("noida-{}", QUERY_ID.fetch_add(1, Ordering::Relaxed)));

    match engine::execute(&query) {
        Ok((result, fmt_from_query)) => {
            let format = fmt_from_query
                .or_else(|| req.query.get("default_format").cloned())
                .unwrap_or_else(|| "TabSeparated".to_string());
            match format::render(&result, &format) {
                Some(body) => success_response(&result, &format, query_id, body),
                None => error_response(&ChError::not_implemented(&format!("Format {format}"))),
            }
        }
        Err(e) => error_response(&e),
    }
}

fn success_response(
    result: &engine::QueryResult,
    format: &str,
    query_id: String,
    body: Vec<u8>,
) -> Response {
    let rows = result.rows.len();
    Response {
        status: 200,
        headers: vec![
            ("Content-Type".into(), format::content_type(format).into()),
            ("X-ClickHouse-Query-Id".into(), query_id),
            ("X-ClickHouse-Format".into(), format.to_string()),
            ("X-ClickHouse-Timezone".into(), "UTC".into()),
            ("X-ClickHouse-Server-Display-Name".into(), "noida-db".into()),
            (
                "X-ClickHouse-Summary".into(),
                format!(
                    "{{\"read_rows\":\"{rows}\",\"read_bytes\":\"0\",\"written_rows\":\"0\",\
                     \"written_bytes\":\"0\",\"total_rows_to_read\":\"0\",\"result_rows\":\"{rows}\",\
                     \"result_bytes\":\"0\",\"elapsed_ns\":\"0\"}}"
                ),
            ),
        ],
        body,
    }
}

/// `query` is either the `query` URL param, or — if that's empty — the raw
/// body, matching how `clickhouse-client` and the HTTP client libraries
/// send it.
fn query_text(req: &Request) -> Option<String> {
    match req.query.get("query") {
        Some(q) if !q.is_empty() => Some(q.clone()),
        _ if !req.body.is_empty() => Some(String::from_utf8_lossy(&req.body).into_owned()),
        _ => None,
    }
}

fn error_response(e: &ChError) -> Response {
    Response {
        status: e.http_status(),
        headers: vec![
            ("X-ClickHouse-Exception-Code".into(), e.code.to_string()),
            ("Content-Type".into(), "text/plain; charset=UTF-8".into()),
        ],
        body: e.body().into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_replies_ok() {
        let req = Request {
            method: "GET".into(),
            path: "/ping".into(),
            query: Default::default(),
            body: vec![],
        };
        let resp = route(&req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"Ok.\n");
    }

    #[test]
    fn select_one_over_get() {
        let mut query = std::collections::HashMap::new();
        query.insert("query".to_string(), "SELECT 1".to_string());
        let req = Request { method: "GET".into(), path: "/".into(), query, body: vec![] };
        let resp = route(&req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"1\n");
    }

    #[test]
    fn unknown_table_is_404_with_exception_code_header() {
        let mut query = std::collections::HashMap::new();
        query.insert("query".to_string(), "SELECT * FROM nope".to_string());
        let req = Request { method: "GET".into(), path: "/".into(), query, body: vec![] };
        let resp = route(&req);
        assert_eq!(resp.status, 404);
        assert!(resp.headers.iter().any(|(k, v)| k == "X-ClickHouse-Exception-Code" && v == "60"));
    }

    #[test]
    fn query_via_post_body() {
        let req = Request {
            method: "POST".into(),
            path: "/".into(),
            query: Default::default(),
            body: b"SELECT 1".to_vec(),
        };
        let resp = route(&req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"1\n");
    }
}
