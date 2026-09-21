//! TCP front end: one thread per connection, one engine behind a mutex.
//! Simple on purpose; noida is for local development.

use std::io::{self, BufReader, BufWriter, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use super::engine::{ClientConn, Engine, Session};
use super::resp::{self, ReadError, Value};

/// Connection threads touch little stack; keep reservations small.
const STACK_SIZE: usize = 256 * 1024;

/// How often a blocked connection re-checks its socket and timeout.
const BLOCKED_POLL: Duration = Duration::from_millis(100);

/// The engine, plus a condition variable that blocked connections wait on.
/// It is notified whenever a command may have produced replies for them.
struct Shared {
    engine: Mutex<Engine>,
    replies: Condvar,
}

/// Binds `addr` and serves Redis on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let engine = Arc::new(Shared { engine: Mutex::new(Engine::new()), replies: Condvar::new() });

    let sweeper = Arc::clone(&engine);
    thread::Builder::new().name("redis-expire".into()).stack_size(STACK_SIZE).spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            sweeper.engine.lock().unwrap().purge_expired();
        }
    })?;

    thread::Builder::new()
        .name("redis-accept".into())
        .stack_size(STACK_SIZE)
        .spawn(move || accept_loop(listener, engine))?;
    Ok(local)
}

fn accept_loop(listener: TcpListener, engine: Arc<Shared>) {
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

fn handle(stream: TcpStream, engine: Arc<Shared>) -> io::Result<()> {
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
    let mut session = engine.engine.lock().unwrap().connect(conn);
    let result = serve(stream, &engine, &mut session);
    engine.engine.lock().unwrap().disconnect(&session);
    result
}

fn serve(stream: TcpStream, shared: &Shared, session: &mut Session) -> io::Result<()> {
    let peer = stream.try_clone()?;
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
        let mut reply = loop {
            let mut e = shared.engine.lock().unwrap();
            if !e.is_paused_for(&args) {
                let reply = e.execute(session, &args);
                if e.has_replies() {
                    shared.replies.notify_all();
                }
                break reply;
            }
            drop(e);
            // CLIENT PAUSE: hold the command until the pause ends.
            writer.flush()?;
            thread::sleep(Duration::from_millis(10));
        };
        if session.blocked {
            writer.flush()?;
            match wait_unblocked(shared, session, &peer) {
                Some(r) => reply = r,
                None => return Ok(()),
            }
        }
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

/// Waits for a blocked command's reply: served by another client's write,
/// timed out, or unblocked by CLIENT UNBLOCK. `None` if the client went
/// away meanwhile (disconnected or killed).
fn wait_unblocked(shared: &Shared, session: &mut Session, peer: &TcpStream) -> Option<Value> {
    let mut e = shared.engine.lock().unwrap();
    loop {
        if let Some(reply) = e.take_reply(session) {
            return Some(reply);
        }
        let deadline = e.block_deadline(session.id)?;
        if e.expire_blocked() {
            shared.replies.notify_all();
            continue;
        }
        let mut wait = BLOCKED_POLL;
        if deadline != 0 {
            let left = (deadline + 1).saturating_sub(e.now());
            wait = wait.min(Duration::from_millis(left.max(1)));
        }
        e = shared.replies.wait_timeout(e, wait).unwrap().0;
        if e.has_reply_for(session.id) {
            continue;
        }
        // Like Redis, notice a client that disconnects while blocked, so it
        // can't swallow data meant for the next waiter.
        if closed(peer) {
            return None;
        }
    }
}

/// Whether the peer has closed its end, without consuming any input.
fn closed(s: &TcpStream) -> bool {
    if s.set_nonblocking(true).is_err() {
        return false;
    }
    let mut buf = [0u8; 1];
    let eof = matches!(s.peek(&mut buf), Ok(0));
    let _ = s.set_nonblocking(false);
    eof
}
