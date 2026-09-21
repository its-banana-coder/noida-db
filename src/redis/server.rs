//! TCP front end: one thread per connection, one engine behind a mutex.
//! Simple on purpose; noida is for local development.

use std::io::{self, BufReader, BufWriter, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::engine::{ClientConn, Engine};
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
    for stream in listener.incoming().flatten() {
        let engine = Arc::clone(&engine);
        let _ = thread::Builder::new().name("redis-conn".into()).stack_size(STACK_SIZE).spawn(
            move || {
                let _ = handle(stream, engine);
            },
        );
    }
}

#[cfg(unix)]
fn fd_of(s: &TcpStream) -> i64 {
    use std::os::fd::AsRawFd;
    s.as_raw_fd() as i64
}

#[cfg(not(unix))]
fn fd_of(_: &TcpStream) -> i64 {
    0
}

fn handle(stream: TcpStream, engine: Arc<Mutex<Engine>>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let killer = stream.try_clone()?;
    let conn = ClientConn {
        addr: stream.peer_addr()?.to_string(),
        laddr: stream.local_addr()?.to_string(),
        fd: fd_of(&stream),
        kill: Some(Box::new(move || {
            let _ = killer.shutdown(Shutdown::Both);
        })),
    };
    let mut session = engine.lock().unwrap().connect(conn);
    let result = serve(stream, &engine, &mut session);
    engine.lock().unwrap().disconnect(&session);
    result
}

fn serve(
    stream: TcpStream,
    engine: &Mutex<Engine>,
    session: &mut super::Session,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    let mut out = Vec::new();
    loop {
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(ReadError::Io(e)) => return Err(e),
            Err(ReadError::Protocol(e)) => {
                out.clear();
                let err = Value::err(format!("ERR Protocol error: {}", e.0));
                resp::encode(&err, session.resp, &mut out);
                writer.write_all(&out)?;
                return writer.flush();
            }
        };
        if args.is_empty() {
            continue;
        }
        let reply = loop {
            let mut e = engine.lock().unwrap();
            if !e.is_paused_for(&args) {
                break e.execute(session, &args);
            }
            drop(e);
            // CLIENT PAUSE: hold the command until the pause ends.
            writer.flush()?;
            thread::sleep(Duration::from_millis(10));
        };
        out.clear();
        resp::encode(&reply, session.resp, &mut out);
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
