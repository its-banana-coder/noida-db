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
    let mut length = 0usize;
    let mut product = None;
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap();
            }
            if name.eq_ignore_ascii_case("x-elastic-product") {
                product = Some(value.trim().to_owned());
            }
        }
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).unwrap();
    let body = if bytes.is_empty() { Value::Null } else { serde_json::from_slice(&bytes).unwrap() };
    Reply { status, product, body }
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
