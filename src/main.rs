//! noida: one tiny local binary standing in for Postgres, MySQL, Redis,
//! Kafka, Elasticsearch and ClickHouse during development.

mod config;

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
    println!(
        "noida {} | data dir: {}",
        env!("CARGO_PKG_VERSION"),
        cfg.data_dir.display()
    );
    for svc in &cfg.services {
        println!(
            "  {:<14} {}:{}  (not implemented yet)",
            svc.name, cfg.host, svc.port
        );
    }
    println!("press Ctrl-C to stop");
    // No listeners exist yet; park so `noida start` behaves like a server.
    loop {
        std::thread::park();
    }
}
