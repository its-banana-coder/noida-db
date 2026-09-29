use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::engine::Engine;

const STACK_SIZE: usize = 256 * 1024;

struct Shared {
    engine: Mutex<Engine>,
}

pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;

    let engine = Engine::default();
    let shared = Arc::new(Shared { engine: Mutex::new(engine) });

    let sweeper = Arc::clone(&shared);
    thread::Builder::new()
        .name("memcached-expire".into())
        .stack_size(STACK_SIZE)
        .spawn(move || loop {
            thread::sleep(Duration::from_secs(1));
            sweeper.engine.lock().unwrap().purge_expired();
        })?;

    let accepter = Arc::clone(&shared);
    thread::Builder::new()
        .name("memcached-accept".into())
        .stack_size(STACK_SIZE)
        .spawn(move || accept_loop(listener, accepter))?;

    Ok(local)
}

fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    for stream in listener.incoming().flatten() {
        let shared = Arc::clone(&shared);
        let _ = thread::Builder::new()
            .name("memcached-conn".into())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let _ = handle(stream, shared);
            });
    }
}

fn read_exact_chunk(reader: &mut BufReader<TcpStream>, bytes: usize) -> io::Result<Option<Vec<u8>>> {
    let mut buf = vec![0; bytes];
    if let Err(_) = reader.read_exact(&mut buf) {
        return Ok(None);
    }
    let mut crlf = [0; 2];
    if let Err(_) = reader.read_exact(&mut crlf) {
        return Ok(None);
    }
    if crlf != [b'\r', b'\n'] {
        // Read until \n to recover
        let mut trash = Vec::new();
        let _ = reader.read_until(b'\n', &mut trash);
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad data chunk"));
    }
    Ok(Some(buf))
}

fn write_out(writer: &mut TcpStream, noreply: bool, msg: &[u8]) -> io::Result<()> {
    if !noreply {
        writer.write_all(msg)?;
    }
    Ok(())
}

fn handle(stream: TcpStream, shared: Arc<Shared>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                let parsed_line = line.trim_end_matches("\r\n");
                if parsed_line.is_empty() {
                    continue;
                }
                let mut parts = parsed_line.split_whitespace();
                let cmd = parts.next().unwrap_or("");

                let mut noreply = false;
                let args: Vec<&str> = parts.collect();
                if let Some(&"noreply") = args.last() {
                    noreply = true;
                }

                // Parse and dispatch
                let res = dispatch_command(cmd, &args, noreply, &mut reader, &shared);
                match res {
                    Ok(reply) => {
                        if !reply.is_empty() {
                            write_out(&mut writer, false, &reply)?;
                        }
                    },
                    Err(e) => {
                        // Error string returned. Write it if not a hard IO error.
                        if e.kind() == io::ErrorKind::InvalidData {
                            write_out(&mut writer, false, format!("CLIENT_ERROR {}\r\n", e.into_inner().unwrap()).as_bytes())?;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
}

fn dispatch_command(
    cmd: &str,
    args: &[&str],
    noreply: bool,
    reader: &mut BufReader<TcpStream>,
    shared: &Arc<Shared>
) -> io::Result<Vec<u8>> {
    match cmd {
        "set" | "add" | "replace" | "append" | "prepend" => {
            if args.len() < 4 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let key = args[0].as_bytes().to_vec();
            if key.len() > 250 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let flags: u32 = args[1].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;
            let exptime: u32 = args[2].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;
            let bytes: usize = args[3].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;

            if bytes > 1024 * 1024 {
                // Read and discard to recover the stream, but we don't allocate a big buffer.
                let mut left = bytes + 2;
                let mut buf = [0; 4096];
                while left > 0 {
                    let to_read = left.min(buf.len());
                    if let Err(_) = reader.read_exact(&mut buf[..to_read]) {
                        break;
                    }
                    left -= to_read;
                }
                return Ok(b"SERVER_ERROR object too large for cache\r\n".to_vec());
            }

            let data = match read_exact_chunk(reader, bytes) {
                Ok(Some(d)) => d,
                Ok(None) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
                Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad data chunk")),
            };

            let mut e = shared.engine.lock().unwrap();
            let res = match cmd {
                "set" => e.set(key, flags, exptime, data).map(|_| b"STORED\r\n".to_vec()),
                "add" => e.add(key, flags, exptime, data).map(|_| b"STORED\r\n".to_vec()),
                "replace" => e.replace(key, flags, exptime, data).map(|_| b"STORED\r\n".to_vec()),
                "append" => e.append(&key, &data).map(|_| b"STORED\r\n".to_vec()),
                "prepend" => e.prepend(&key, &data).map(|_| b"STORED\r\n".to_vec()),
                _ => unreachable!(),
            };

            if noreply {
                return Ok(vec![]);
            }
            match res {
                Ok(msg) => Ok(msg),
                Err(_) => Ok(b"NOT_STORED\r\n".to_vec()),
            }
        },
        "cas" => {
            if args.len() < 5 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let key = args[0].as_bytes().to_vec();
            if key.len() > 250 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let flags: u32 = args[1].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;
            let exptime: u32 = args[2].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;
            let bytes: usize = args[3].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;
            let cas_unique: u64 = args[4].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;

            if bytes > 1024 * 1024 {
                let mut left = bytes + 2;
                let mut buf = [0; 4096];
                while left > 0 {
                    let to_read = left.min(buf.len());
                    if let Err(_) = reader.read_exact(&mut buf[..to_read]) {
                        break;
                    }
                    left -= to_read;
                }
                return Ok(b"SERVER_ERROR object too large for cache\r\n".to_vec());
            }

            let data = match read_exact_chunk(reader, bytes) {
                Ok(Some(d)) => d,
                Ok(None) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
                Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "bad data chunk")),
            };

            let mut e = shared.engine.lock().unwrap();
            let res = e.cas(key, flags, exptime, data, cas_unique);

            if noreply {
                return Ok(vec![]);
            }
            match res {
                Ok(_) => Ok(b"STORED\r\n".to_vec()),
                Err(true) => Ok(b"EXISTS\r\n".to_vec()), // cas conflict
                Err(false) => Ok(b"NOT_FOUND\r\n".to_vec()),
            }
        },
        "get" | "gets" | "gat" | "gats" => {
            let wants_cas = cmd == "gets" || cmd == "gats";
            let wants_touch = cmd == "gat" || cmd == "gats";

            let mut keys_start = 0;
            let mut exptime = 0;
            if wants_touch {
                if args.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
                }
                exptime = args[0].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;
                keys_start = 1;
            }

            let mut e = shared.engine.lock().unwrap();
            let mut reply = Vec::new();
            for &key_str in &args[keys_start..] {
                let key = key_str.as_bytes();
                if key.len() > 250 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
                }

                if wants_touch {
                    let _ = e.touch(key, exptime);
                }

                if let Some(item) = e.get(key) {
                    if wants_cas {
                        reply.extend_from_slice(format!("VALUE {} {} {} {}\r\n", key_str, item.flags, item.data.len(), item.cas).as_bytes());
                    } else {
                        reply.extend_from_slice(format!("VALUE {} {} {}\r\n", key_str, item.flags, item.data.len()).as_bytes());
                    }
                    reply.extend_from_slice(&item.data);
                    reply.extend_from_slice(b"\r\n");
                }
            }
            reply.extend_from_slice(b"END\r\n");
            Ok(reply)
        },
        "delete" => {
            if args.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format.  Usage: delete <key> [noreply]"));
            }
            let key = args[0].as_bytes();
            if key.len() > 250 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            // handle legacy delete <key> 0
            if args.len() >= 2 && args[1] != "noreply" {
                if args[1] != "0" {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format.  Usage: delete <key> [noreply]"));
                }
            }

            let mut e = shared.engine.lock().unwrap();
            let res = e.delete(key);
            if noreply {
                return Ok(vec![]);
            }
            match res {
                Ok(_) => Ok(b"DELETED\r\n".to_vec()),
                Err(_) => Ok(b"NOT_FOUND\r\n".to_vec()),
            }
        },
        "incr" | "decr" => {
            if args.len() < 2 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let key = args[0].as_bytes();
            if key.len() > 250 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let val: u64 = match args[1].parse() {
                Ok(v) => v,
                Err(_) => return Ok(b"CLIENT_ERROR invalid numeric delta argument\r\n".to_vec()),
            };

            let mut e = shared.engine.lock().unwrap();
            let res = e.incr_decr(key, val, cmd == "incr");
            if noreply {
                return Ok(vec![]);
            }
            match res {
                Ok(new_val) => Ok(format!("{}\r\n", new_val).into_bytes()),
                Err(Ok(_)) => Ok(b"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n".to_vec()),
                Err(Err(_)) => Ok(b"NOT_FOUND\r\n".to_vec()),
            }
        },
        "touch" => {
            if args.len() < 2 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let key = args[0].as_bytes();
            if key.len() > 250 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
            }
            let exptime: u32 = args[1].parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad command line format"))?;

            let mut e = shared.engine.lock().unwrap();
            let res = e.touch(key, exptime);
            if noreply {
                return Ok(vec![]);
            }
            match res {
                Ok(_) => Ok(b"TOUCHED\r\n".to_vec()),
                Err(_) => Ok(b"NOT_FOUND\r\n".to_vec()),
            }
        },
        "flush_all" => {
            let mut delay = 0;
            if args.len() >= 1 && args[0] != "noreply" {
                if let Ok(d) = args[0].parse() {
                    delay = d;
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "bad command line format"));
                }
            }

            let mut e = shared.engine.lock().unwrap();
            e.flush_all(delay);
            if noreply {
                return Ok(vec![]);
            }
            Ok(b"OK\r\n".to_vec())
        },
        "version" => {
            Ok(b"VERSION 1.6.29\r\n".to_vec())
        },
        "verbosity" => {
            if noreply {
                Ok(vec![])
            } else {
                Ok(b"OK\r\n".to_vec())
            }
        },
        "quit" => {
            Ok(vec![])
        },
        "stats" => {
            // Very minimal stats response
            let reply = b"STAT pid 1\r\n\
STAT uptime 100\r\n\
STAT time 1000000000\r\n\
STAT version 1.6.29\r\n\
STAT libevent 2.1.12-stable\r\n\
STAT pointer_size 64\r\n\
STAT max_connections 1024\r\n\
STAT curr_connections 1\r\n\
STAT total_connections 1\r\n\
STAT cmd_get 0\r\n\
STAT cmd_set 0\r\n\
STAT get_hits 0\r\n\
STAT get_misses 0\r\n\
STAT curr_items 0\r\n\
STAT total_items 0\r\n\
STAT bytes 0\r\n\
STAT limit_maxbytes 67108864\r\n\
STAT threads 4\r\n\
STAT evictions 0\r\n\
END\r\n";
            Ok(reply.to_vec())
        },
        "cache_memlimit" => Ok(b"OK\r\n".to_vec()),
        "shutdown" => Ok(b"ERROR: shutdown not enabled\r\n".to_vec()),
        "misbehave" | "lru_crawler" | "slabs" | "watch" => {
            Ok(b"ERROR\r\n".to_vec())
        },
        _ => {
            Ok(b"ERROR\r\n".to_vec())
        }
    }
}
