#![cfg(feature = "elasticsearch")]

use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};

struct Reply {
    status: u16,
    body: Value,
}

fn request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> Reply {
    let stream = TcpStream::connect(addr).unwrap();
    let mut writer = stream.try_clone().unwrap();
    write!(writer, "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", method, path, addr, body.len()).unwrap();
    writer.write_all(body).unwrap();
    writer.flush().unwrap();

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let status = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut length: Option<usize> = None;
    let mut chunked = false;
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
        }
    }

    let bytes = if method == "HEAD" {
        Vec::new()
    } else if chunked {
        let mut out = Vec::new();
        loop {
            let mut size_line = String::new();
            reader.read_line(&mut size_line).unwrap();
            let size = usize::from_str_radix(size_line.trim(), 16).unwrap();
            if size == 0 {
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
    Reply { status, body }
}

#[test]
fn elasticsearch_persistence_save_and_load() {
    let dir = std::env::temp_dir().join(format!("noida-es-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    // Start ephemeral behavior on empty dir
    let addr1 =
        noida::services::start_persistent("elasticsearch", "127.0.0.1:0", &dir).unwrap().unwrap();

    // Create an index
    let create = br#"{"settings":{"index":{"number_of_shards":"1"}}, "mappings": {"properties": {"title": {"type": "text"}}}}"#;
    let res = request(addr1, "PUT", "/my_index", create);
    assert_eq!(res.status, 200);

    // Index documents
    let doc1 = br#"{"title":"document one"}"#;
    let res = request(addr1, "PUT", "/my_index/_doc/1", doc1);
    assert_eq!(res.status, 201);
    let doc2 = br#"{"title":"document two"}"#;
    let res = request(addr1, "PUT", "/my_index/_doc/2", doc2);
    assert_eq!(res.status, 201);

    // Trigger save
    noida::persistence::run_shutdown_hooks_for_test();

    // Verify snapshot file exists
    assert!(dir.join("elasticsearch.json").exists());

    // Start a second server pointing at the same dir
    let addr2 =
        noida::services::start_persistent("elasticsearch", "127.0.0.1:0", &dir).unwrap().unwrap();

    // Verify mappings
    let res = request(addr2, "GET", "/my_index/_mapping", b"");
    assert_eq!(res.status, 200);
    assert!(res.body["my_index"]["properties"]["title"].is_object());

    // Verify documents via search
    let search = br#"{"query":{"match_all":{}}}"#;
    let res = request(addr2, "POST", "/my_index/_search", search);
    assert_eq!(res.status, 200);
    let total = res.body["hits"]["total"]["value"].as_u64().unwrap_or(0);
    assert_eq!(total, 2);

    let _ = fs::remove_dir_all(&dir);
}
