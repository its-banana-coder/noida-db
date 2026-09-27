//! Kafka compatibility: native Kafka binary protocol on port 9092.

pub mod codec;
pub mod connection;
pub mod engine;

#[cfg(test)]
mod tests;

use std::io;
use std::net::{SocketAddr, TcpListener};
use std::thread;

use engine::Engine;

/// Binds `addr` and serves Kafka on background threads.
pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    let engine = Engine::new(local_addr.ip().to_string(), local_addr.port() as i32);

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let engine = engine.clone();
            thread::spawn(move || {
                connection::handle_connection(stream, engine);
            });
        }
    });

    Ok(local_addr)
}
