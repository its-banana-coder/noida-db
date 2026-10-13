//! noida-db: one tiny local binary standing in for Postgres, MySQL, Redis,
//! Kafka, Elasticsearch, ClickHouse and MongoDB during
//! development.

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
        Command::Version => println!("noida-db {}", env!("CARGO_PKG_VERSION")),
        #[cfg(feature = "ui")]
        Command::Ui { data_dir } => {
            if let Err(err) = noida::ui::run(data_dir) {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        }
        #[cfg(not(feature = "ui"))]
        Command::Ui { .. } => {
            eprintln!("error: this build has no terminal panel (rebuild with --features ui)");
            return ExitCode::FAILURE;
        }
        Command::Start(cfg) => {
            if let Err(err) = start(&cfg) {
                eprintln!("error: {err}");
                return ExitCode::FAILURE;
            }
        }
        other => return noida::cli::run(other),
    }
    ExitCode::SUCCESS
}

fn start(cfg: &Config) -> std::io::Result<()> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    // Two servers on one data dir would overwrite each other's snapshots.
    if let Some(other) = noida::control::Client::find(&cfg.data_dir) {
        return Err(std::io::Error::other(format!(
            "noida-db is already running for {} (pid {}); see `noida-db status`",
            cfg.data_dir.display(),
            other.pid
        )));
    }
    println!("noida-db {} | data dir: {}", env!("CARGO_PKG_VERSION"), cfg.data_dir.display());
    // Before starting any service, so a persistent service's own
    // `spawn_persistent` can register its shutdown-save hook as soon as
    // it starts rather than racing a signal that arrives before this is
    // installed.
    noida::persistence::install_shutdown_handler();
    noida::services::configure(cfg);
    for svc in &cfg.services {
        let (s, result) = noida::services::start_service(svc.name);
        match result {
            Ok(()) => {
                println!("  {:<14} {}", svc.name, s.to_json()["connect"].as_str().unwrap_or(""))
            }
            Err(_) if s.state == "unavailable" => {
                println!("  {:<14} {}:{}  (not in this build)", svc.name, cfg.host, svc.port)
            }
            Err(msg) => {
                let hint = if msg.contains("in use") {
                    format!(
                        "\nhint: pick another port with --{}-port <n>, or leave it out with --only",
                        svc.name
                    )
                } else {
                    String::new()
                };
                return Err(std::io::Error::other(format!("{}: {msg}{hint}", svc.name)));
            }
        }
    }
    // The control API (`noida-db status`, `stop <service>`, `ui`, ...).
    noida::control::serve(&cfg.data_dir)?;
    println!("press Ctrl-C to stop");
    // Services run on background threads; keep the process alive.
    loop {
        std::thread::park();
    }
}
