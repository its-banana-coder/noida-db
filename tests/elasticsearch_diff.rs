//! Small HTTP differential suite for the P0 Elasticsearch surface.
//!
//! Set `NOIDA_ELASTICSEARCH_REF=http://host:port` to compare against a real
//! Elasticsearch 8.15 node. It is intentionally useful with only the first
//! milestone implemented, and grows alongside the engine.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

use serde_json::{Value, json};

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

/// A small e-commerce-shaped product fixture, shared by the tests below --
/// `term`/`terms`/`bool`/`range`/`exists` are all implemented
/// (`src/elasticsearch/search.rs`) but were previously only verified by
/// this crate's own internal unit tests, never against a real node.
fn seed_products(real_addr: SocketAddr, ours_addr: SocketAddr, index: &str) {
    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    let create = br#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"#;
    expect_ok("real: PUT index", &request(real_addr, "PUT", index, create));
    expect_ok("ours: PUT index", &request(ours_addr, "PUT", index, create));

    let docs: [(&str, &[u8]); 5] = [
        ("101", br#"{"name":"Apple MacBook Pro 14","category":"laptops","brand":"Apple","price":189999,"discount_price":174999,"rating":4.8,"tags":["laptop","apple","professional"]}"#),
        ("102", br#"{"name":"Apple MacBook Air","category":"laptops","brand":"Apple","price":124999,"discount_price":114999,"rating":4.7,"tags":["laptop","apple","ultrabook"]}"#),
        ("103", br#"{"name":"Dell XPS 14","category":"laptops","brand":"Dell","price":149999,"rating":4.5,"tags":["laptop","windows","premium"]}"#),
        ("104", br#"{"name":"Sony WH-1000XM6","category":"headphones","brand":"Sony","price":39999,"discount_price":34999,"rating":4.6,"tags":["headphones","wireless","noise-cancelling"]}"#),
        ("105", br#"{"name":"Sony WH-CH720N","category":"headphones","brand":"Sony","price":12999,"rating":4.3,"tags":["headphones","wireless","budget"]}"#),
    ];
    for (id, d) in docs.iter() {
        let path = format!("{index}/_doc/{id}");
        expect_ok("real: PUT doc", &request(real_addr, "PUT", &path, d));
        expect_ok("ours: PUT doc", &request(ours_addr, "PUT", &path, d));
    }
    expect_ok("real: refresh", &request(real_addr, "POST", &format!("{index}/_refresh"), b""));
    expect_ok("ours: refresh", &request(ours_addr, "POST", &format!("{index}/_refresh"), b""));
}

#[test]
fn p1_term_bool_range_exists_queries_match_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_term_bool_range";
    seed_products(real_addr, ours_addr, index);

    compare_query(
        "term brand",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"term":{"brand":"Apple"}}}"#,
        false,
    );
    compare_query(
        "terms brand",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"terms":{"brand":["Apple","Dell"]}}}"#,
        false,
    );
    compare_query(
        "range price",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"range":{"price":{"gte":100000,"lte":150000}}}}"#,
        false,
    );
    compare_query(
        "exists discount_price",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"exists":{"field":"discount_price"}}}"#,
        false,
    );
    compare_query(
        "bool must+filter+must_not",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"bool":{"must":[{"match":{"name":"wireless"}}],"filter":[{"term":{"brand":"Sony"}},{"range":{"price":{"lte":40000}}}],"must_not":[{"term":{"tags":"budget"}}]}}}"#,
        false,
    );
    compare_query(
        "bool should",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"bool":{"should":[{"term":{"brand":"Apple"}},{"term":{"brand":"Dell"}}]}}}"#,
        false,
    );
    compare_query(
        "bool must_not",
        real_addr,
        ours_addr,
        index,
        br#"{"query":{"bool":{"must_not":[{"term":{"brand":"Sony"}}]}}}"#,
        false,
    );

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!("compared term/terms/range/exists/bool against real Elasticsearch");
}

#[test]
fn p1_sort_pagination_source_filtering_match_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_sort_pagination";
    seed_products(real_addr, ours_addr, index);

    // Single-field sort: must compare exact ORDER, not just the matched
    // id set, so this calls _search directly rather than compare_query.
    for (label, body) in [
        ("sort price asc", br#"{"query":{"match_all":{}},"sort":[{"price":"asc"}]}"# as &[u8]),
        ("sort price desc", br#"{"query":{"match_all":{}},"sort":[{"price":"desc"}]}"#),
        (
            "multi-field sort",
            br#"{"query":{"match_all":{}},"sort":[{"rating":"desc"},{"price":"asc"}]}"#,
        ),
    ] {
        let real = request(real_addr, "POST", &format!("{index}/_search"), body);
        let ours = request(ours_addr, "POST", &format!("{index}/_search"), body);
        expect_ok(&format!("real: {label}"), &real);
        expect_ok(&format!("ours: {label}"), &ours);
        let real_ids: Vec<&str> = real.body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["_id"].as_str().unwrap())
            .collect();
        let ours_ids: Vec<&str> = ours.body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["_id"].as_str().unwrap())
            .collect();
        assert_eq!(ours_ids, real_ids, "{label}: order mismatch");
    }

    // Pagination: from/size pages must partition the full (sorted) result
    // set with no overlap and no gaps.
    let mut seen = std::collections::HashSet::new();
    for from in [0, 2, 4] {
        let body = format!(
            r#"{{"query":{{"match_all":{{}}}},"sort":[{{"price":"asc"}}],"from":{from},"size":2}}"#
        );
        let real = request(real_addr, "POST", &format!("{index}/_search"), body.as_bytes());
        let ours = request(ours_addr, "POST", &format!("{index}/_search"), body.as_bytes());
        expect_ok("real: page", &real);
        expect_ok("ours: page", &ours);
        let real_ids: Vec<&str> = real.body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["_id"].as_str().unwrap())
            .collect();
        let ours_ids: Vec<&str> = ours.body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["_id"].as_str().unwrap())
            .collect();
        assert_eq!(ours_ids, real_ids, "page from={from}: mismatch");
        for id in &real_ids {
            assert!(seen.insert(id.to_string()), "page from={from}: id {id} seen twice");
        }
    }
    assert_eq!(seen.len(), 5, "pagination should cover all 5 seeded products exactly once");

    // _source filtering.
    let body = br#"{"query":{"match_all":{}},"_source":["name","price"]}"#;
    let real = request(real_addr, "POST", &format!("{index}/_search"), body);
    let ours = request(ours_addr, "POST", &format!("{index}/_search"), body);
    expect_ok("real: source filter", &real);
    expect_ok("ours: source filter", &ours);
    for h in ours.body["hits"]["hits"].as_array().unwrap() {
        let src = h["_source"].as_object().unwrap();
        let mut keys: Vec<&str> = src.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["name", "price"], "_source filter leaked extra fields");
    }

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!("compared sort/pagination/_source filtering against real Elasticsearch");
}

#[test]
fn p1_aggregations_match_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_aggregations";
    seed_products(real_addr, ours_addr, index);

    for (label, body) in [
        ("terms brand", br#"{"size":0,"aggs":{"brands":{"terms":{"field":"brand"}}}}"# as &[u8]),
        ("avg price", br#"{"size":0,"aggs":{"avg_price":{"avg":{"field":"price"}}}}"#),
        ("stats price", br#"{"size":0,"aggs":{"price_stats":{"stats":{"field":"price"}}}}"#),
        (
            "filter agg",
            br#"{"size":0,"aggs":{"sony":{"filter":{"term":{"brand":"Sony"}},"aggs":{"avg_price":{"avg":{"field":"price"}}}}}}"#,
        ),
        (
            "nested terms+avg",
            br#"{"size":0,"aggs":{"brands":{"terms":{"field":"brand"},"aggs":{"avg_price":{"avg":{"field":"price"}},"avg_rating":{"avg":{"field":"rating"}}}}}}"#,
        ),
        (
            "filtered query scopes aggs",
            br#"{"query":{"term":{"category":"laptops"}},"size":0,"aggs":{"brands":{"terms":{"field":"brand"}}}}"#,
        ),
    ] {
        let real = request(real_addr, "POST", &format!("{index}/_search"), body);
        let ours = request(ours_addr, "POST", &format!("{index}/_search"), body);
        expect_ok(&format!("real: {label}"), &real);
        expect_ok(&format!("ours: {label}"), &ours);
        assert_eq!(
            ours.body["aggregations"], real.body["aggregations"],
            "{label}: aggregations mismatch. real: {}, ours: {}",
            real.body["aggregations"], ours.body["aggregations"]
        );
    }

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!("compared terms/avg/stats/filter/nested aggregations against real Elasticsearch");
}

#[test]
fn p1_update_bulk_delete_match_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_update_bulk_delete";
    seed_products(real_addr, ours_addr, index);

    // Partial update.
    let update = br#"{"doc":{"price":169999}}"#;
    let real_u = request(real_addr, "POST", &format!("{index}/_update/101"), update);
    let ours_u = request(ours_addr, "POST", &format!("{index}/_update/101"), update);
    assert_eq!(ours_u.status, real_u.status, "update status");
    assert_eq!(ours_u.body["result"], real_u.body["result"], "update result");

    let real_get = request(real_addr, "GET", &format!("{index}/_doc/101"), b"");
    let ours_get = request(ours_addr, "GET", &format!("{index}/_doc/101"), b"");
    assert_eq!(ours_get.body["_source"]["price"], real_get.body["_source"]["price"]);
    assert_eq!(
        ours_get.body["_source"]["name"], real_get.body["_source"]["name"],
        "update must not touch unrelated fields"
    );

    // Bulk: mixed index/update/delete in one request.
    let bulk = concat!(
        "{\"index\":{\"_id\":\"201\"}}\n",
        "{\"name\":\"Bulk Product\",\"price\":5000}\n",
        "{\"update\":{\"_id\":\"102\"}}\n",
        "{\"doc\":{\"price\":99999}}\n",
        "{\"delete\":{\"_id\":\"105\"}}\n",
    );
    let real_b = request(real_addr, "POST", &format!("{index}/_bulk"), bulk.as_bytes());
    let ours_b = request(ours_addr, "POST", &format!("{index}/_bulk"), bulk.as_bytes());
    assert_eq!(ours_b.body["errors"], real_b.body["errors"], "bulk errors flag");
    expect_ok("real: refresh", &request(real_addr, "POST", &format!("{index}/_refresh"), b""));
    expect_ok("ours: refresh", &request(ours_addr, "POST", &format!("{index}/_refresh"), b""));

    let real_count = request(real_addr, "GET", &format!("{index}/_count"), b"");
    let ours_count = request(ours_addr, "GET", &format!("{index}/_count"), b"");
    assert_eq!(ours_count.body["count"], real_count.body["count"], "count after bulk");
    assert_eq!(ours_count.body["count"], json!(5), "5 original - 1 deleted + 1 bulk-indexed");

    let real_105 = request(real_addr, "GET", &format!("{index}/_doc/105"), b"");
    let ours_105 = request(ours_addr, "GET", &format!("{index}/_doc/105"), b"");
    assert_eq!(ours_105.status, real_105.status);
    assert_eq!(ours_105.body["found"], json!(false), "bulk delete should have removed 105");

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!("compared update/bulk/delete against real Elasticsearch");
}

/// Zero-downtime alias switch: an alias pointed at one index, searched,
/// repointed at a different index, searched again -- the search must
/// follow the alias, not the name it was first created against.
#[test]
fn p1_aliases_match_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let v1 = "/noida_diff_alias_v1";
    let v2 = "/noida_diff_alias_v2";
    let _ = request(real_addr, "DELETE", v1, b"");
    let _ = request(ours_addr, "DELETE", v1, b"");
    let _ = request(real_addr, "DELETE", v2, b"");
    let _ = request(ours_addr, "DELETE", v2, b"");
    let create = br#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"#;
    for addr in [real_addr, ours_addr] {
        expect_ok("PUT v1", &request(addr, "PUT", v1, create));
        expect_ok("PUT v2", &request(addr, "PUT", v2, create));
        expect_ok(
            "doc v1",
            &request(addr, "PUT", "/noida_diff_alias_v1/_doc/1", br#"{"tag":"v1"}"#),
        );
        expect_ok(
            "doc v2",
            &request(addr, "PUT", "/noida_diff_alias_v2/_doc/1", br#"{"tag":"v2"}"#),
        );
        expect_ok("refresh v1", &request(addr, "POST", "/noida_diff_alias_v1/_refresh", b""));
        expect_ok("refresh v2", &request(addr, "POST", "/noida_diff_alias_v2/_refresh", b""));
    }

    let point_at_v1 =
        br#"{"actions":[{"add":{"index":"noida_diff_alias_v1","alias":"noida_diff_alias_current"}}]}"#;
    expect_ok("real: alias->v1", &request(real_addr, "POST", "/_aliases", point_at_v1));
    expect_ok("ours: alias->v1", &request(ours_addr, "POST", "/_aliases", point_at_v1));

    compare_query(
        "search via alias (v1)",
        real_addr,
        ours_addr,
        "/noida_diff_alias_current",
        br#"{"query":{"match_all":{}}}"#,
        false,
    );
    let real1 = request(real_addr, "GET", "/noida_diff_alias_current/_doc/1", b"");
    let ours1 = request(ours_addr, "GET", "/noida_diff_alias_current/_doc/1", b"");
    assert_eq!(ours1.body["_source"]["tag"], real1.body["_source"]["tag"]);
    assert_eq!(ours1.body["_source"]["tag"], json!("v1"));

    // Zero-downtime switch: remove from v1, add to v2, in one request.
    let switch = br#"{"actions":[{"remove":{"index":"noida_diff_alias_v1","alias":"noida_diff_alias_current"}},{"add":{"index":"noida_diff_alias_v2","alias":"noida_diff_alias_current"}}]}"#;
    expect_ok("real: alias->v2", &request(real_addr, "POST", "/_aliases", switch));
    expect_ok("ours: alias->v2", &request(ours_addr, "POST", "/_aliases", switch));

    let real2 = request(real_addr, "GET", "/noida_diff_alias_current/_doc/1", b"");
    let ours2 = request(ours_addr, "GET", "/noida_diff_alias_current/_doc/1", b"");
    assert_eq!(ours2.body["_source"]["tag"], real2.body["_source"]["tag"]);
    assert_eq!(ours2.body["_source"]["tag"], json!("v2"), "alias should now resolve to v2");

    let _ = request(real_addr, "DELETE", v1, b"");
    let _ = request(ours_addr, "DELETE", v1, b"");
    let _ = request(real_addr, "DELETE", v2, b"");
    let _ = request(ours_addr, "DELETE", v2, b"");
    println!("compared alias search + zero-downtime alias switch against real Elasticsearch");
}

/// Optimistic concurrency: an update against a stale `_seq_no`/
/// `_primary_term` must be rejected with a real version conflict.
#[test]
fn p1_optimistic_concurrency_matches_real_elasticsearch() {
    let Ok(reference) = std::env::var("NOIDA_ELASTICSEARCH_REF") else {
        eprintln!("SKIPPED: no reference Elasticsearch (set NOIDA_ELASTICSEARCH_REF)");
        return;
    };
    let real_addr = endpoint(&reference);
    let ours_addr =
        noida::elasticsearch::spawn("127.0.0.1:0").expect("start noida-db Elasticsearch");
    let index = "/noida_diff_optimistic_concurrency";
    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    let create = br#"{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"#;
    expect_ok("real: PUT index", &request(real_addr, "PUT", index, create));
    expect_ok("ours: PUT index", &request(ours_addr, "PUT", index, create));

    for addr in [real_addr, ours_addr] {
        expect_ok("PUT doc", &request(addr, "PUT", &format!("{index}/_doc/1"), br#"{"n":1}"#));
    }

    let real_get = request(real_addr, "GET", &format!("{index}/_doc/1"), b"");
    let ours_get = request(ours_addr, "GET", &format!("{index}/_doc/1"), b"");
    let real_seq = real_get.body["_seq_no"].as_i64().unwrap();
    let real_term = real_get.body["_primary_term"].as_i64().unwrap();
    let ours_seq = ours_get.body["_seq_no"].as_i64().unwrap();
    let ours_term = ours_get.body["_primary_term"].as_i64().unwrap();

    // First update using the correct seq_no/primary_term succeeds.
    let path_real = format!("{index}/_doc/1?if_seq_no={real_seq}&if_primary_term={real_term}");
    let path_ours = format!("{index}/_doc/1?if_seq_no={ours_seq}&if_primary_term={ours_term}");
    let real_ok = request(real_addr, "PUT", &path_real, br#"{"n":2}"#);
    let ours_ok = request(ours_addr, "PUT", &path_ours, br#"{"n":2}"#);
    assert_eq!(ours_ok.status, real_ok.status, "first conditional update should succeed on both");
    assert!((200..300).contains(&ours_ok.status), "first conditional update should succeed");

    // Reusing the now-stale seq_no/primary_term must conflict on both.
    let real_conflict = request(real_addr, "PUT", &path_real, br#"{"n":3}"#);
    let ours_conflict = request(ours_addr, "PUT", &path_ours, br#"{"n":3}"#);
    assert_eq!(
        real_conflict.status, 409,
        "real Elasticsearch itself should reject the stale write"
    );
    assert_eq!(
        ours_conflict.status, 409,
        "noida-db should reject the stale write the same way: {}",
        ours_conflict.body
    );

    let _ = request(real_addr, "DELETE", index, b"");
    let _ = request(ours_addr, "DELETE", index, b"");
    println!(
        "compared optimistic concurrency (if_seq_no/if_primary_term) against real Elasticsearch"
    );
}
