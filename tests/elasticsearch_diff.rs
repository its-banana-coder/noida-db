//! Small HTTP differential suite for the P0 Elasticsearch surface.
//!
//! Set `NOIDA_ELASTICSEARCH_REF=http://host:port` to compare against a real
//! Elasticsearch 8.15 node. It is intentionally useful with only the first
//! milestone implemented, and grows alongside the engine.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

use serde_json::Value;

struct Reply {
    status: u16,
    product: Option<String>,
    body: Value,
}

fn endpoint(url: &str) -> SocketAddr {
    let authority = url.strip_prefix("http://").unwrap_or(url).trim_end_matches('/');
    let authority =
        if authority.contains(':') { authority.to_owned() } else { format!("{authority}:9200") };
    authority.to_socket_addrs().unwrap().next().expect("resolve reference Elasticsearch")
}

fn request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> Reply {
    let stream = TcpStream::connect(addr).expect("connect Elasticsearch");
    let mut writer = stream.try_clone().unwrap();
    write!(writer, "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len()).unwrap();
    writer.write_all(body).unwrap();
    writer.flush().unwrap();

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let status = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut length: Option<usize> = None;
    let mut chunked = false;
    let mut product = None;
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse().unwrap());
            }
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
            if name.eq_ignore_ascii_case("x-elastic-product") {
                product = Some(value.trim().to_owned());
            }
        }
    }
    // Real Elasticsearch's HTTP layer sends chunked responses for at
    // least some endpoints (observed on _search) — this client has to
    // decode that framing itself rather than assume Content-Length is
    // always present, or it silently reads zero bytes and treats a real,
    // successful response as an empty body.
    //
    // A HEAD response never has a body, full stop, regardless of what its
    // Content-Length/Transfer-Encoding headers claim (they describe what a
    // GET to the same resource would return) — trying to decode one as
    // chunked reads an empty chunk-size line and panics.
    let bytes = if method == "HEAD" {
        Vec::new()
    } else if chunked {
        let mut out = Vec::new();
        loop {
            let mut size_line = String::new();
            reader.read_line(&mut size_line).unwrap();
            let size = usize::from_str_radix(size_line.trim(), 16).unwrap();
            if size == 0 {
                // Trailing headers (if any), then the final CRLF.
                loop {
                    let mut trailer = String::new();
                    reader.read_line(&mut trailer).unwrap();
                    if trailer == "\r\n" {
                        break;
                    }
                }
                break;
            }
            let mut chunk = vec![0; size];
            reader.read_exact(&mut chunk).unwrap();
            out.extend_from_slice(&chunk);
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf).unwrap();
        }
        out
    } else {
        let mut buf = vec![0; length.unwrap_or(0)];
        reader.read_exact(&mut buf).unwrap();
        buf
    };
    let body = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap() };
    Reply { status, product, body }
}

/// Panics with the full status/body if `r` isn't a 2xx — every call site
/// that writes to the *real* server used to ignore its return value
/// entirely, so a silent failure there (index creation racing a still-
/// in-flight delete, a transient error) would only surface much later as
/// a confusing mismatch on some unrelated field, instead of here where
/// the actual problem is.
fn expect_ok(what: &str, r: &Reply) {
    assert!((200..300).contains(&r.status), "{what} failed: {} {}", r.status, r.body);
}

fn normalize_root(mut body: Value) -> Value {
    body.as_object_mut().unwrap().remove("name");
    body.as_object_mut().unwrap().remove("cluster_uuid");
    let version = body["version"].as_object_mut().unwrap();
    version.remove("build_hash");
    version.remove("build_date");
    body
}

fn compare(label: &str, real: Reply, ours: Reply, compared: &mut usize) {
    *compared += 1;
    assert_eq!(ours.status, real.status, "{label}: status");
    assert_eq!(ours.product.as_deref(), Some("Elasticsearch"), "{label}: product header");
    let (real_body, ours_body) = if label == "GET /" {
        (normalize_root(real.body), normalize_root(ours.body))
    } else {
        (real.body, ours.body)
    };
    assert_eq!(ours_body, real_body, "{label}: response body");
}

#[test]
fn p0_index_management_matches_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_books";
    let mut compared = 0;

    // The CI reference is long lived enough that a prior failed run may have
    // left this index behind. Reset both servers before comparing P0 calls.
    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");

    compare(
        "GET /",
        request(real_addr, "GET", "/", b""),
        request(ours_addr, "GET", "/", b""),
        &mut compared,
    );
    let create = br#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"#;
    compare(
        "PUT index",
        request(real_addr, "PUT", index, create),
        request(ours_addr, "PUT", index, create),
        &mut compared,
    );
    compare(
        "HEAD index",
        request(real_addr, "HEAD", index, b""),
        request(ours_addr, "HEAD", index, b""),
        &mut compared,
    );
    compare(
        "DELETE index",
        request(real_addr, "DELETE", index, b""),
        request(ours_addr, "DELETE", index, b""),
        &mut compared,
    );
    println!("compared {compared} Elasticsearch P0 replies");
}

/// BM25 relevance is the part of the spec most likely to be subtly wrong
/// (Lucene's lossy field-length norm encoding, 32-bit float arithmetic) —
/// eyeballing the formula doesn't catch that, comparing `_score` against a
/// real node does.
#[test]
fn p0_search_bm25_scoring_matches_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_search";
    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    let create = br#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"#;
    expect_ok("real: PUT index", &request(real_addr, "PUT", index, create));
    expect_ok("ours: PUT index", &request(ours_addr, "PUT", index, create));

    let docs: [&[u8]; 4] = [
        br#"{"title":"the quick brown fox","pages":100}"#,
        br#"{"title":"quick quick fox fox jumps","pages":220}"#,
        br#"{"title":"an entirely unrelated book","pages":50}"#,
        br#"{"title":"fox fox fox fox fox","pages":400}"#,
    ];
    for (i, d) in docs.iter().enumerate() {
        let path = format!("{index}/_doc/{}", i + 1);
        expect_ok("real: PUT doc", &request(real_addr, "PUT", &path, d));
        expect_ok("ours: PUT doc", &request(ours_addr, "PUT", &path, d));
    }
    expect_ok("real: refresh", &request(real_addr, "POST", &format!("{index}/_refresh"), b""));
    expect_ok("ours: refresh", &request(ours_addr, "POST", &format!("{index}/_refresh"), b""));

    let query = br#"{"query":{"match":{"title":"quick fox"}}}"#;
    let real = request(real_addr, "POST", &format!("{index}/_search"), query);
    let ours = request(ours_addr, "POST", &format!("{index}/_search"), query);
    expect_ok("real: search", &real);
    expect_ok("ours: search", &ours);
    assert_eq!(ours.status, real.status);
    assert_eq!(
        ours.body["hits"]["total"]["value"], real.body["hits"]["total"]["value"],
        "real search response was: {}",
        real.body
    );

    let real_hits = real.body["hits"]["hits"].as_array().expect("real hits");
    let ours_hits = ours.body["hits"]["hits"].as_array().expect("our hits");
    assert_eq!(real_hits.len(), ours_hits.len());
    let mut compared = 0;
    for (r, o) in real_hits.iter().zip(ours_hits.iter()) {
        assert_eq!(r["_id"], o["_id"], "ranking order must match");
        let rs = r["_score"].as_f64().unwrap();
        let os = o["_score"].as_f64().unwrap();
        let tolerance = (rs.abs() * 1e-5).max(1e-6);
        assert!(
            (rs - os).abs() <= tolerance,
            "_score mismatch for id {:?}: real={rs} ours={os}",
            r["_id"]
        );
        compared += 1;
    }

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!("compared BM25 _score for {compared} hits against real Elasticsearch");
}

/// Compares one query's hit ids (order-independent — real Elasticsearch's
/// tie-breaking among equally-scored docs isn't specified/guaranteed to
/// match ours) and, for scored queries, each hit's `_score` within a
/// tolerance the same way the BM25 test above does.
fn compare_query(
    label: &str,
    real_addr: SocketAddr,
    ours_addr: SocketAddr,
    index: &str,
    query: &[u8],
    check_scores: bool,
) {
    let real = request(real_addr, "POST", &format!("{index}/_search"), query);
    let ours = request(ours_addr, "POST", &format!("{index}/_search"), query);
    expect_ok(&format!("real: {label}"), &real);
    expect_ok(&format!("ours: {label}"), &ours);
    assert_eq!(
        ours.body["hits"]["total"]["value"], real.body["hits"]["total"]["value"],
        "{label}: hit count mismatch. real response was: {}",
        real.body
    );

    let mut real_ids: Vec<String> = real.body["hits"]["hits"]
        .as_array()
        .expect("real hits")
        .iter()
        .map(|h| h["_id"].as_str().unwrap().to_string())
        .collect();
    let mut ours_ids: Vec<String> = ours.body["hits"]["hits"]
        .as_array()
        .expect("our hits")
        .iter()
        .map(|h| h["_id"].as_str().unwrap().to_string())
        .collect();
    real_ids.sort();
    ours_ids.sort();
    assert_eq!(ours_ids, real_ids, "{label}: matched id set mismatch");

    if check_scores {
        let real_by_id: std::collections::HashMap<&str, f64> = real.body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| (h["_id"].as_str().unwrap(), h["_score"].as_f64().unwrap()))
            .collect();
        for h in ours.body["hits"]["hits"].as_array().unwrap() {
            let id = h["_id"].as_str().unwrap();
            let os = h["_score"].as_f64().unwrap();
            let rs = real_by_id[id];
            let tolerance = (rs.abs() * 1e-5).max(1e-6);
            assert!(
                (rs - os).abs() <= tolerance,
                "{label}: _score mismatch for id {id:?}: real={rs} ours={os}"
            );
        }
    }
}

/// The four query types added after the P0 milestones
/// (match_phrase/multi_match/wildcard/regexp) were only verified
/// self-consistently when they landed (no Docker in that sandbox) — this
/// is the real-server verification for them, run wherever
/// NOIDA_ELASTICSEARCH_REF is a real node (CI's service container).
#[test]
fn p0_new_query_types_match_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_query_types";
    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    let create = br#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"#;
    expect_ok("real: PUT index", &request(real_addr, "PUT", index, create));
    expect_ok("ours: PUT index", &request(ours_addr, "PUT", index, create));

    let mapping = br#"{"properties":{"sku":{"type":"keyword"}}}"#;
    expect_ok(
        "real: PUT mapping",
        &request(real_addr, "PUT", &format!("{index}/_mapping"), mapping),
    );
    expect_ok(
        "ours: PUT mapping",
        &request(ours_addr, "PUT", &format!("{index}/_mapping"), mapping),
    );

    let docs: [(&str, &[u8]); 6] = [
        ("1", br#"{"body":"the quick brown fox jumps","title":"rust programming","sku":"BOOK-1234"}"#),
        ("2", br#"{"body":"a fox that is quick and brown","title":"a book","sku":"BOOK-5678"}"#),
        ("3", br#"{"body":"quick brown fox","title":"cooking","sku":"DISC-1234"}"#),
        ("4", br#"{"body":"no matches here","title":"rust rust rust systems programming","sku":"AB-100"}"#),
        ("5", br#"{"body":"unrelated","title":"unrelated","sku":"AB-250"}"#),
        ("6", br#"{"body":"unrelated","title":"unrelated","sku":"CD-100"}"#),
    ];
    for (id, d) in docs.iter() {
        let path = format!("{index}/_doc/{id}");
        expect_ok("real: PUT doc", &request(real_addr, "PUT", &path, d));
        expect_ok("ours: PUT doc", &request(ours_addr, "PUT", &path, d));
    }
    expect_ok("real: refresh", &request(real_addr, "POST", &format!("{index}/_refresh"), b""));
    expect_ok("ours: refresh", &request(ours_addr, "POST", &format!("{index}/_refresh"), b""));

    compare_query(
        "match_phrase",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"match_phrase":{"body":"quick brown fox"}}}"#,
        true,
    );
    compare_query(
        "multi_match best_fields",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"multi_match":{"query":"rust","fields":["title","body"]}}}"#,
        true,
    );
    compare_query(
        "multi_match operator=and",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"multi_match":{"query":"quick fox","fields":["title","body"],"operator":"and"}}}"#,
        false,
    );
    compare_query(
        "wildcard prefix",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"wildcard":{"sku":"BOOK-*"}}}"#,
        false,
    );
    compare_query(
        "wildcard single-char",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"wildcard":{"sku":"BOOK-1?34"}}}"#,
        false,
    );
    compare_query(
        "regexp",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"regexp":{"sku":"AB-[0-9]+"}}}"#,
        false,
    );
    compare_query(
        "regexp anchored",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"regexp":{"sku":"AB-1[0-9]{2}"}}}"#,
        false,
    );

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!("compared match_phrase/multi_match/wildcard/regexp against real Elasticsearch");
}
