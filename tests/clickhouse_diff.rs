//! Differential tests: every query runs against a real ClickHouse server and
//! noida-db over HTTP, and the response bodies must match byte for byte
//! (after normalizing query ids and timings).
//!
//! The reference server is `NOIDA_CLICKHOUSE_REF=host:port` (CI points this
//! at `clickhouse/clickhouse-server:24.8`; there's no local install and no
//! Docker here, so with the env var unset this prints SKIPPED and passes).

mod common;

use std::io::Read;
use std::net::SocketAddr;

/// (query, format) pairs to compare as text.
const QUERIES: &[(&str, &str)] = &[
    ("SELECT 1", "TabSeparated"),
    ("SELECT 1", "TabSeparatedWithNamesAndTypes"),
    ("SELECT version()", "TabSeparated"),
    ("SELECT * FROM system.one", "TabSeparated"),
    ("SELECT number FROM numbers(5)", "TabSeparated"),
    ("SELECT 1", "JSONEachRow"),
    ("SELECT k, count(*), sum(v) FROM diff_t GROUP BY k ORDER BY k", "TabSeparated"),
    ("SELECT v FROM diff_t WHERE v > 1 ORDER BY v", "TabSeparated"),
    ("SELECT id, v FROM diff_rmt FINAL ORDER BY id", "TabSeparated"),
    ("SELECT k, amount FROM diff_smt FINAL ORDER BY k", "TabSeparated"),
];

/// (query, format) pairs compared as raw bytes (binary formats).
const BINARY_QUERIES: &[(&str, &str)] =
    &[("SELECT number FROM numbers(5)", "RowBinaryWithNamesAndTypes")];

/// Run once against each server before comparing `QUERIES`, so the
/// GROUP BY/WHERE/FINAL queries above have data to read.
const SETUP: &[&str] = &[
    "DROP TABLE IF EXISTS diff_t",
    "CREATE TABLE diff_t (k String, v UInt32) ENGINE = Memory",
    "INSERT INTO diff_t VALUES ('a', 1), ('a', 2), ('b', 10)",
    "DROP TABLE IF EXISTS diff_rmt",
    "CREATE TABLE diff_rmt (id UInt32, v String, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY (id)",
    "INSERT INTO diff_rmt VALUES (1, 'old', 1), (1, 'new', 2), (2, 'x', 1)",
    "DROP TABLE IF EXISTS diff_smt",
    "CREATE TABLE diff_smt (k String, amount UInt32) ENGINE = SummingMergeTree ORDER BY (k)",
    "INSERT INTO diff_smt VALUES ('a', 1), ('a', 2), ('b', 10)",
];

fn http_get(addr: &str, query: &str, format: &str) -> (u16, String) {
    let (status, body) = http_get_bytes(addr, query, format);
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn http_get_bytes(addr: &str, query: &str, format: &str) -> (u16, Vec<u8>) {
    let url = format!("http://{addr}/?query={}&default_format={format}", urlencode(query));
    let mut resp =
        ureq::get(&url).config().http_status_as_error(false).build().call().expect("request");
    let status = resp.status().as_u16();
    let mut body = Vec::new();
    resp.body_mut().as_reader().read_to_end(&mut body).expect("read body");
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

    for stmt in SETUP {
        http_get(&reference, stmt, "TabSeparated");
        http_get(&noida, stmt, "TabSeparated");
    }

    let mut compared = 0;
    for (query, format) in QUERIES {
        let (want_status, want_body) = http_get(&reference, query, format);
        let (got_status, got_body) = http_get(&noida, query, format);
        assert_eq!(want_status, got_status, "status differs for {query} FORMAT {format}");
        assert_eq!(want_body, got_body, "body differs for {query} FORMAT {format}");
        compared += 1;
    }
    for (query, format) in BINARY_QUERIES {
        let (want_status, want_body) = http_get_bytes(&reference, query, format);
        let (got_status, got_body) = http_get_bytes(&noida, query, format);
        assert_eq!(want_status, got_status, "status differs for {query} FORMAT {format}");
        assert_eq!(want_body, got_body, "body differs for {query} FORMAT {format}");
        compared += 1;
    }
    println!("{compared} queries compared identical to real ClickHouse");
}
