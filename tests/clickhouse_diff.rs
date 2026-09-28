//! Differential tests: every query runs against a real ClickHouse server and
//! noida-db over HTTP, and the response bodies must match byte for byte
//! (after normalizing query ids and timings).
//!
//! The reference server is `NOIDA_CLICKHOUSE_REF=host:port` (CI points this
//! at `clickhouse/clickhouse-server:24.8`; there's no local install and no
//! Docker here, so with the env var unset this prints SKIPPED and passes).

mod common;

use std::net::SocketAddr;

/// (query, format) pairs milestone 1 covers.
const QUERIES: &[(&str, &str)] = &[
    ("SELECT 1", "TabSeparated"),
    ("SELECT 1", "TabSeparatedWithNamesAndTypes"),
    ("SELECT version()", "TabSeparated"),
    ("SELECT * FROM system.one", "TabSeparated"),
    ("SELECT number FROM numbers(5)", "TabSeparated"),
    ("SELECT 1", "JSONEachRow"),
];

fn http_get(addr: &str, query: &str, format: &str) -> (u16, String) {
    let url = format!("http://{addr}/?query={}&default_format={format}", urlencode(query));
    let mut resp =
        ureq::get(&url).config().http_status_as_error(false).build().call().expect("request");
    let status = resp.status().as_u16();
    let body = resp.body_mut().read_to_string().expect("read body");
    (status, body)
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            b' ' => "%20".to_string(),
            b => format!("%{b:02X}"),
        })
        .collect()
}

#[test]
fn queries_match_real_clickhouse() {
    let Ok(reference) = std::env::var("NOIDA_CLICKHOUSE_REF") else {
        println!("SKIPPED: set NOIDA_CLICKHOUSE_REF=host:port to run against a real ClickHouse");
        return;
    };
    let noida: SocketAddr = common::start_noida_clickhouse();
    let noida = noida.to_string();

    let mut compared = 0;
    for (query, format) in QUERIES {
        let (want_status, want_body) = http_get(&reference, query, format);
        let (got_status, got_body) = http_get(&noida, query, format);
        assert_eq!(want_status, got_status, "status differs for {query} FORMAT {format}");
        assert_eq!(want_body, got_body, "body differs for {query} FORMAT {format}");
        compared += 1;
    }
    println!("{compared} queries compared identical to real ClickHouse");
}
