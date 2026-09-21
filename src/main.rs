//! noida: one tiny local binary standing in for Postgres, MySQL, Redis,
//! Kafka, Elasticsearch and ClickHouse during development.

use noida::config;

use std::process::ExitCode;

use config::{Command, Config};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match config::parse(&args) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("error: {err}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };

    match command {
        Command::Help => println!("{}", config::USAGE),
        Command::Version => println!("noida {}", env!("CARGO_PKG_VERSION")),
        Command::Start(cfg) => {
            if let Err(err) = start(&cfg) {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}

fn start(cfg: &Config) -> std::io::Result<()> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    println!("noida {} | data dir: {}", env!("CARGO_PKG_VERSION"), cfg.data_dir.display());
    for svc in &cfg.services {
        let addr = format!("{}:{}", cfg.host, svc.port);
        match noida::services::start(svc.name, &addr) {
            Some(Ok(bound)) => println!("  {:<14} {bound}", svc.name),
            Some(Err(e)) => return Err(io_context(e, svc.name, &addr)),
            None => println!("  {:<14} {addr}  (not in this build)", svc.name),
        }
    }
    println!("press Ctrl-C to stop");
    // Services run on background threads; keep the process alive.
    loop {
        std::thread::park();
    }
}

fn io_context(e: std::io::Error, service: &str, addr: &str) -> std::io::Error {
    std::io::Error::new(e.kind(), format!("{service}: cannot listen on {addr}: {e}"))
}
