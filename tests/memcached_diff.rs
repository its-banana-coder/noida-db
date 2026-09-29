//! Differential tests for memcached text protocol.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[cfg(feature = "memcached")]
fn start_noida_memcached() -> SocketAddr {
    noida::memcached::spawn("127.0.0.1:0").expect("spawn memcached")
}

#[cfg(feature = "memcached")]
fn get_reference_server() -> Option<SocketAddr> {
    if let Ok(addr) = std::env::var("NOIDA_MEMCACHED_REF") {
        return addr.parse().ok();
    }
    // Try connecting to a locally running memcached on default port.
    if let Ok(stream) =
        TcpStream::connect_timeout(&"127.0.0.1:11211".parse().unwrap(), Duration::from_millis(50))
    {
        let _ = stream;
        return Some("127.0.0.1:11211".parse().unwrap());
    }
    None
}

#[cfg(feature = "memcached")]
fn run_script(addr: SocketAddr, script: &[&str]) -> Vec<String> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let mut results = Vec::new();
    let mut i = 0;
    while i < script.len() {
        let mut cmd = format!("{}\r\n", script[i]);
        let line = script[i];

        // If this is a storage command (set, add, replace, append, prepend, cas) we must write the chunk as well before reading response
        let is_storage = line.starts_with("set ")
            || line.starts_with("add ")
            || line.starts_with("replace ")
            || line.starts_with("append ")
            || line.starts_with("prepend ")
            || line.starts_with("cas ");

        if is_storage {
            i += 1;
            let payload = format!("{}\r\n", script[i]);
            cmd.push_str(&payload);
        }

        stream.write_all(cmd.as_bytes()).unwrap();

        let mut buf = [0; 4096];
        let mut response = String::new();

        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    response.push_str(std::str::from_utf8(&buf[..n]).unwrap());
                    // Heuristic for complete responses for standard tests
                    if response.ends_with("END\r\n")
                        || response.ends_with("STORED\r\n")
                        || response.ends_with("NOT_STORED\r\n")
                        || response.ends_with("EXISTS\r\n")
                        || response.ends_with("NOT_FOUND\r\n")
                        || response.ends_with("DELETED\r\n")
                        || response.ends_with("OK\r\n")
                        || response.ends_with("TOUCHED\r\n")
                        || response.ends_with("CLIENT_ERROR\r\n")
                        || response.ends_with("ERROR\r\n")
                        || (line.starts_with("incr ") && response.ends_with("\r\n"))
                        || (line.starts_with("decr ") && response.ends_with("\r\n"))
                        || (line.starts_with("version") && response.ends_with("\r\n"))
                    {
                        break;
                    }
                    if line.starts_with("stats") && response.ends_with("END\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        // Strip stats specific values which change between runs/servers.
        if line.starts_with("stats") {
            let mut cleaned = Vec::new();
            for l in response.lines() {
                if l.starts_with("STAT") {
                    let parts: Vec<&str> = l.split_whitespace().collect();
                    if parts.len() == 3 {
                        // Keep only the key name to verify the field exists, ignore values
                        cleaned.push(format!("STAT {}", parts[1]));
                    } else {
                        cleaned.push(l.to_string());
                    }
                } else {
                    cleaned.push(l.to_string());
                }
            }
            results.push(cleaned.join("\n"));
        } else {
            results.push(response);
        }
        i += 1;
    }
    results
}

#[cfg(feature = "memcached")]
#[test]
fn diff_basic_ops() {
    let ref_server = match get_reference_server() {
        Some(s) => s,
        None => {
            println!("SKIPPED: no reference server");
            return;
        }
    };

    let noida_addr = start_noida_memcached();

    let script = vec![
        "flush_all",
        "set k1 0 0 5",
        "hello",
        "get k1",
        "add k1 0 0 2",
        "hi",
        "replace k1 0 0 2",
        "hi",
        "append k1 0 0 2",
        "ya",
        "prepend k1 0 0 1",
        "o",
        "get k1",
        "delete k1",
        "get k1",
        "set num 0 0 1",
        "5",
        "incr num 2",
        "decr num 1",
        "delete num",
        "stats",
    ];

    let ref_out = run_script(ref_server, &script);
    let noida_out = run_script(noida_addr, &script);

    for (i, (r, n)) in ref_out.iter().zip(noida_out.iter()).enumerate() {
        if r != n {
            panic!("Mismatch at step {}:\nReference:\n{}\n\nNoida:\n{}", i, r, n);
        }
    }
}
