//! Stopping one running service without stopping the process: an accept
//! loop that can be told to end, plus a registry of the service's open
//! connections so they can be closed.
//!
//! A service's accept loop iterates `life.incoming(listener)` instead of
//! `listener.incoming()`, and keeps the `Conn` each step yields alive for as
//! long as it serves that connection (by moving it into the connection's
//! thread). `stop` then ends the loop (dropping the listener, so the port
//! is really closed), shuts down every open connection, and waits briefly
//! for their threads to finish, so the service's engine can be dropped.

use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// How long `stop` waits for the accept loop and the open connections to
/// let go before giving up (a connection stuck in a long query finishes on
/// its own later; its socket is already shut down).
const STOP_WAIT: Duration = Duration::from_secs(3);

/// One service's run: cloned into its accept loop and background threads.
#[derive(Clone, Default)]
pub struct Lifecycle(Arc<Inner>);

#[derive(Default)]
struct Inner {
    stopped: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    /// The listener's address, while the accept loop holds it.
    listening: Option<SocketAddr>,
    next_id: u64,
    /// A clone of each open connection's socket, to shut it down on stop.
    conns: HashMap<u64, TcpStream>,
}

impl Lifecycle {
    /// Whether `stop` was called. Background threads (expiry sweepers and
    /// the like) poll this to end with their service.
    pub fn is_stopped(&self) -> bool {
        self.0.stopped.load(Ordering::SeqCst)
    }

    /// Accepts connections on `listener` until `stop`. Call it before
    /// spawning the accept thread, so a `stop` right after startup still
    /// finds the listener.
    pub fn incoming(&self, listener: TcpListener) -> Incoming {
        self.lock().listening = listener.local_addr().ok();
        Incoming { life: self.clone(), listener: Some(listener) }
    }

    /// The number of connections currently open.
    pub fn connections(&self) -> usize {
        self.lock().conns.len()
    }

    /// Ends the accept loop (closing the port), shuts down every open
    /// connection and waits for them to close. Safe to call twice.
    pub fn stop(&self) {
        self.0.stopped.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + STOP_WAIT;
        let mut st = self.lock();
        // The accept loop is blocked in `accept`; a connection of our own
        // wakes it to see the flag. Retried, in case one gets lost.
        while let Some(addr) = st.listening {
            drop(st);
            let _ = TcpStream::connect_timeout(&wake_addr(addr), Duration::from_millis(200));
            st = self.lock();
            if Instant::now() >= deadline {
                break;
            }
            if st.listening.is_some() {
                st = self.0.changed.wait_timeout(st, Duration::from_millis(50)).unwrap().0;
            }
        }
        for stream in st.conns.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        while !st.conns.is_empty() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            st = self.0.changed.wait_timeout(st, deadline - now).unwrap().0;
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Where to connect to reach a listener bound to `addr`: a wildcard
/// address (0.0.0.0, ::) is reached through loopback.
fn wake_addr(addr: SocketAddr) -> SocketAddr {
    let mut addr = addr;
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            SocketAddr::V4(_) => [127, 0, 0, 1].into(),
            SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    addr
}

/// The accept loop: yields each new connection with its `Conn` guard, and
/// ends (dropping the listener) once the service is stopped.
pub struct Incoming {
    life: Lifecycle,
    listener: Option<TcpListener>,
}

impl Incoming {
    fn close(&mut self) {
        if self.listener.take().is_some() {
            self.life.lock().listening = None;
            self.life.0.changed.notify_all();
        }
    }
}

impl Iterator for Incoming {
    type Item = (TcpStream, Conn);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let accepted = self.listener.as_ref()?.accept();
            if self.life.is_stopped() {
                self.close();
                return None;
            }
            let Ok((stream, _)) = accepted else {
                // Out of file descriptors and the like: don't spin.
                std::thread::sleep(Duration::from_millis(10));
                continue;
            };
            let mut st = self.life.lock();
            let id = st.next_id;
            st.next_id += 1;
            if let Ok(clone) = stream.try_clone() {
                st.conns.insert(id, clone);
            }
            drop(st);
            return Some((stream, Conn { life: self.life.clone(), id }));
        }
    }
}

impl Drop for Incoming {
    fn drop(&mut self) {
        self.close();
    }
}

/// An open connection's registration: dropping it (when the connection's
/// thread ends) forgets the connection. Holding the socket clone any longer
/// would keep the socket open after the service is done with it.
pub struct Conn {
    life: Lifecycle,
    id: u64,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.life.lock().conns.remove(&self.id);
        self.life.0.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// An echo service on the lifecycle, the way each real service uses it.
    fn echo(life: &Lifecycle) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = life.incoming(listener);
        std::thread::spawn(move || {
            for (mut stream, conn) in incoming {
                std::thread::spawn(move || {
                    let _conn = conn;
                    let mut buf = [0u8; 64];
                    while let Ok(n @ 1..) = stream.read(&mut buf) {
                        let _ = stream.write_all(&buf[..n]);
                    }
                });
            }
        });
        addr
    }

    #[test]
    fn stop_closes_the_port_and_open_connections() {
        let life = Lifecycle::default();
        let addr = echo(&life);
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(b"hi").unwrap();
        let mut buf = [0u8; 2];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hi");
        assert_eq!(life.connections(), 1);

        life.stop();
        assert!(life.is_stopped());
        assert_eq!(life.connections(), 0);
        // The open connection sees end-of-stream...
        client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        assert_eq!(client.read(&mut buf).unwrap_or(0), 0);
        // ...and nobody listens on the port any more.
        assert!(TcpStream::connect(addr).is_err());
        // Twice is fine.
        life.stop();
    }

    #[test]
    fn closed_connections_are_forgotten() {
        let life = Lifecycle::default();
        let addr = echo(&life);
        for _ in 0..3 {
            let mut c = TcpStream::connect(addr).unwrap();
            c.write_all(b"x").unwrap();
            let mut b = [0u8; 1];
            c.read_exact(&mut b).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while life.connections() > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(life.connections(), 0);
        life.stop();
    }

    #[test]
    fn stop_before_the_accept_thread_runs() {
        let life = Lifecycle::default();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut incoming = life.incoming(listener);
        let stopper = life.clone();
        let t = std::thread::spawn(move || stopper.stop());
        assert!(incoming.next().is_none());
        t.join().unwrap();
        drop(incoming);
        assert!(TcpStream::connect(addr).is_err());
    }
}
