//! TCP front end: one thread per connection, one engine behind a mutex.
//! Simple on purpose; noida is for local development.

use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::engine::{Engine, Session};
use super::resp::{self, ReadError, Value};

/// Connection threads touch little stack; keep reservations small.
const STACK_SIZE: usize = 256 * 1024;

/// Binds `addr` and serves Redis on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let engine = Arc::new(Mutex::new(Engine::new()));

    let sweeper = Arc::clone(&engine);
    thread::Builder::new().name("redis-expire".into()).stack_size(STACK_SIZE).spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            sweeper.lock().unwrap().purge_expired();
        }
    })?;

    thread::Builder::new()
        .name("redis-accept".into())
        .stack_size(STACK_SIZE)
        .spawn(move || accept_loop(listener, engine))?;
    Ok(local)
}

fn accept_loop(listener: TcpListener, engine: Arc<Mutex<Engine>>) {
    let next_id = AtomicU64::new(1);
    for stream in listener.incoming().flatten() {
        let engine = Arc::clone(&engine);
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let _ = thread::Builder::new()
            .name(format!("redis-conn-{id}"))
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let _ = handle(stream, engine, id);
            });
    }
}

fn handle(stream: TcpStream, engine: Arc<Mutex<Engine>>, id: u64) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    let mut session = Session::new(id);
    let mut out = Vec::new();
    loop {
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(ReadError::Io(e)) => return Err(e),
            Err(ReadError::Protocol(e)) => {
                out.clear();
                resp::encode(&Value::err(format!("ERR Protocol error: {}", e.0)), &mut out);
                writer.write_all(&out)?;
                return writer.flush();
            }
        };
        if args.is_empty() {
            continue;
        }
        let reply = engine.lock().unwrap().execute(&mut session, &args);
        out.clear();
        resp::encode(&reply, &mut out);
        writer.write_all(&out)?;
        if session.closing {
            return writer.flush();
        }
        // Flush once the pipeline drains, not after every reply.
        if reader.buffer().is_empty() {
            writer.flush()?;
        }
    }
}
