//! A real HTTP client (ureq) against noida-db's ClickHouse HTTP interface —
//! the same shape clickhouse-connect (Python) and @clickhouse/client (Node)
//! use. The official Rust `clickhouse` crate needs RowBinaryWithNamesAndTypes
//! (milestone 3); its own client test lands with that format.

mod common;

use std::net::SocketAddr;

/// GETs `query` and returns (status, body), without treating 4xx/5xx as a
/// Rust error — noida-db's ClickHouse-format error bodies are what these
/// tests check.
fn get(addr: SocketAddr, query: &str) -> (u16, String) {
    let url = format!("http://{addr}/?query={}", urlencode(query));
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
fn ping() {
    let addr = common::start_noida_clickhouse();
    let url = format!("http://{addr}/ping");
    let mut resp = ureq::get(&url).call().unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.body_mut().read_to_string().unwrap(), "Ok.\n");
}

#[test]
fn select_one_default_format_is_tab_separated() {
    let addr = common::start_noida_clickhouse();
    let (status, body) = get(addr, "SELECT 1");
    assert_eq!(status, 200);
    assert_eq!(body, "1\n");
}

#[test]
fn select_version() {
    let addr = common::start_noida_clickhouse();
    let (status, body) = get(addr, "SELECT version()");
    assert_eq!(status, 200);
    assert_eq!(body.trim_end(), "24.8.4.13");
}

#[test]
fn json_each_row_format() {
    let addr = common::start_noida_clickhouse();
    let url = format!("http://{addr}/?query={}&default_format=JSONEachRow", urlencode("SELECT 1"));
    let mut resp = ureq::get(&url).call().unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.body_mut().read_to_string().unwrap(), "{\"1\":1}\n");
}

#[test]
fn system_numbers_table_function() {
    let addr = common::start_noida_clickhouse();
    let (status, body) = get(addr, "SELECT number FROM numbers(5)");
    assert_eq!(status, 200);
    assert_eq!(body, "0\n1\n2\n3\n4\n");
}

#[test]
fn unknown_table_error_has_clickhouse_exception_format() {
    let addr = common::start_noida_clickhouse();
    let (status, body) = get(addr, "SELECT * FROM nope");
    assert_eq!(status, 404);
    assert!(body.contains("UNKNOWN_TABLE"), "{body}");
    assert!(body.starts_with("Code: 60."), "{body}");
}

#[test]
fn query_via_post_body() {
    let addr = common::start_noida_clickhouse();
    let url = format!("http://{addr}/");
    let mut resp = ureq::post(&url).send(&b"SELECT 1"[..]).unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.body_mut().read_to_string().unwrap(), "1\n");
}

/// The scenario from docs/specs/clickhouse.md's client matrix: create a
/// table, bulk insert, aggregate with GROUP BY, ORDER BY. One connection
/// pool reusing the same server, the way a real client library does.
#[test]
fn create_insert_group_by_scenario() {
    let addr = common::start_noida_clickhouse();
    let (status, _) = get(
        addr,
        "CREATE TABLE events (kind String, n UInt32) ENGINE = MergeTree() ORDER BY (kind)",
    );
    assert_eq!(status, 200);
    let (status, _) =
        get(addr, "INSERT INTO events (kind, n) VALUES ('click', 1), ('click', 2), ('view', 10)");
    assert_eq!(status, 200);
    let (status, body) =
        get(addr, "SELECT kind, count(*), sum(n) FROM events GROUP BY kind ORDER BY kind");
    assert_eq!(status, 200);
    assert_eq!(body, "click\t2\t3\nview\t1\t10\n");
}

#[test]
fn where_clause_filters_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "CREATE TABLE t (n UInt32) ENGINE = Memory").0, 200);
    assert_eq!(get(addr, "INSERT INTO t VALUES (1), (2), (3)").0, 200);
    let (status, body) = get(addr, "SELECT n FROM t WHERE n > 1 ORDER BY n");
    assert_eq!(status, 200);
    assert_eq!(body, "2\n3\n");
}

#[test]
fn describe_table_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "CREATE TABLE t (id UInt32, name String) ENGINE = Memory").0, 200);
    let (status, body) = get(addr, "DESCRIBE TABLE t");
    assert_eq!(status, 200);
    assert_eq!(body, "id\tUInt32\t\t\t\t\t\nname\tString\t\t\t\t\t\n");
}

#[test]
fn system_columns_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "CREATE TABLE t (id UInt32) ENGINE = MergeTree() ORDER BY (id)").0, 200);
    let (status, body) =
        get(addr, "SELECT database, table, name, type, position FROM system.columns");
    assert_eq!(status, 200);
    assert_eq!(body, "default\tt\tid\tUInt32\t1\n");
}

#[test]
fn show_databases_show_tables_and_exists_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "CREATE TABLE events (id UInt32) ENGINE = Memory").0, 200);

    let (status, body) = get(addr, "SHOW DATABASES");
    assert_eq!(status, 200);
    assert!(body.lines().any(|l| l == "default"), "{body}");

    let (status, body) = get(addr, "SHOW TABLES");
    assert_eq!(status, 200);
    assert_eq!(body, "events\n");

    let (status, body) = get(addr, "EXISTS TABLE events");
    assert_eq!(status, 200);
    assert_eq!(body, "1\n");

    let (status, body) = get(addr, "EXISTS TABLE nope");
    assert_eq!(status, 200);
    assert_eq!(body, "0\n");
}

#[test]
fn show_create_table_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "CREATE TABLE t (id UInt32) ENGINE = MergeTree() ORDER BY (id)").0, 200);
    let (status, body) = get(addr, "SHOW CREATE TABLE t");
    assert_eq!(status, 200);
    assert!(body.contains("CREATE TABLE default.t"), "{body}");
    assert!(body.contains("ENGINE = MergeTree"), "{body}");
    assert!(body.contains("ORDER BY id"), "{body}");
}

#[test]
fn use_and_set_are_accepted_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "USE default").0, 200);
    assert_eq!(get(addr, "SET max_threads = 4").0, 200);
}

#[test]
fn having_clause_over_http() {
    let addr = common::start_noida_clickhouse();
    assert_eq!(get(addr, "CREATE TABLE t (k String, v UInt32) ENGINE = Memory").0, 200);
    assert_eq!(get(addr, "INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10)").0, 200);
    let (status, body) =
        get(addr, "SELECT k, count(*) AS n FROM t GROUP BY k HAVING n > 1 ORDER BY k");
    assert_eq!(status, 200);
    assert_eq!(body, "a\t2\n");
}

#[test]
fn union_all_and_distinct_over_http() {
    let addr = common::start_noida_clickhouse();
    let (status, body) = get(addr, "SELECT 1 AS n UNION ALL SELECT 1 AS n ORDER BY n");
    assert_eq!(status, 200);
    assert_eq!(body, "1\n1\n");

    let (status, body) = get(addr, "SELECT 1 AS n UNION DISTINCT SELECT 1 AS n");
    assert_eq!(status, 200);
    assert_eq!(body, "1\n");
}

#[test]
fn csv_output_format_over_http() {
    let addr = common::start_noida_clickhouse();
    let url = format!(
        "http://{addr}/?query={}&default_format=CSVWithNames",
        urlencode("SELECT 1 AS n, 'hi' AS s")
    );
    let mut resp = ureq::get(&url).call().unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.body_mut().read_to_string().unwrap(), "n,s\n1,hi\n");
}

#[test]
fn pretty_output_format_over_http() {
    let addr = common::start_noida_clickhouse();
    let url = format!("http://{addr}/?query={}&default_format=Pretty", urlencode("SELECT 1 AS n"));
    let mut resp = ureq::get(&url).call().unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.body_mut().read_to_string().unwrap();
    assert!(body.contains('┌'), "{body}");
    assert!(body.contains('1'), "{body}");
}
