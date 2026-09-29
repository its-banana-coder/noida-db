pub mod codec;
pub mod connection;
pub mod engine;
pub mod http;

use engine::Engine;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::thread;

pub fn spawn(addr: &str) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    let engine = Engine::new();
    let http_port = if local_addr.port() == 5672 { 15672 } else { 0 };
    let http_addr = format!("127.0.0.1:{}", http_port);
    if let Ok(_http_local_addr) = http::spawn(&http_addr, engine.clone()) {}
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
