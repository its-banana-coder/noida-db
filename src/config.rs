//! Command-line parsing. Hand-rolled to keep the binary small.

use std::path::PathBuf;

pub const USAGE: &str = "\
usage: noida <command> [options]

commands:
  start      run the server
  version    print the version
  help       print this message

start options:
  --data-dir <path>      where data lives (default: ./.noida)
  --host <addr>          address to bind (default: 127.0.0.1)
  --only <a,b,...>       enable only these services
  --<service>-port <n>   override a port, e.g. --redis-port 6380

services: postgres, mysql, redis, kafka, elasticsearch, clickhouse, memcached,
          mongodb, rabbitmq";

#[derive(Debug, Clone, PartialEq)]
pub struct Service {
    pub name: &'static str,
    pub port: u16,
}

/// Every service noida speaks, on the port its real counterpart uses,
/// so existing clients work with their defaults.
pub const DEFAULT_SERVICES: [Service; 9] = [
    Service { name: "postgres", port: 5432 },
    Service { name: "mysql", port: 3306 },
    Service { name: "redis", port: 6379 },
    Service { name: "kafka", port: 9092 },
    Service { name: "elasticsearch", port: 9200 },
    Service { name: "clickhouse", port: 8123 },
    Service { name: "memcached", port: 11211 },
    Service { name: "mongodb", port: 27017 },
    Service { name: "rabbitmq", port: 5672 },
];

#[derive(Debug, PartialEq)]
pub struct Config {
    pub data_dir: PathBuf,
    pub host: String,
    pub services: Vec<Service>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: PathBuf::from(".noida"),
            host: "127.0.0.1".into(),
            services: DEFAULT_SERVICES.to_vec(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Start(Config),
    Version,
    Help,
}

pub fn parse(args: &[String]) -> Result<Command, String> {
    let Some((cmd, rest)) = args.split_first() else {
        return Ok(Command::Help);
    };
    match cmd.as_str() {
        "start" => parse_start(rest).map(Command::Start),
        "version" | "--version" | "-V" => Ok(Command::Version),
        "help" | "--help" | "-h" => Ok(Command::Help),
        other => Err(format!("unknown command '{other}'")),
    }
}

fn parse_start(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--data-dir" => cfg.data_dir = PathBuf::from(value()?),
            "--host" => cfg.host = value()?.clone(),
            "--only" => {
                let wanted: Vec<&str> = value()?.split(',').map(str::trim).collect();
                for name in &wanted {
                    if !DEFAULT_SERVICES.iter().any(|s| s.name == *name) {
                        return Err(format!("unknown service '{name}'"));
                    }
                }
                cfg.services.retain(|s| wanted.contains(&s.name));
            }
            f => {
                let name = f
                    .strip_prefix("--")
                    .and_then(|f| f.strip_suffix("-port"))
                    .ok_or_else(|| format!("unknown option '{f}'"))?;
                let port: u16 = value()?.parse().map_err(|_| format!("{flag}: invalid port"))?;
                let svc = cfg
                    .services
                    .iter_mut()
                    .find(|s| s.name == name)
                    .ok_or_else(|| format!("unknown service '{name}'"))?;
                svc.port = port;
            }
        }
    }
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }

    fn start(s: &str) -> Config {
        match parse(&args(s)).unwrap() {
            Command::Start(cfg) => cfg,
            other => panic!("expected start, got {other:?}"),
        }
    }

    #[test]
    fn no_args_prints_help() {
        assert_eq!(parse(&[]).unwrap(), Command::Help);
    }

    #[test]
    fn start_uses_defaults() {
        assert_eq!(start("start"), Config::default());
    }

    #[test]
    fn only_filters_services() {
        let cfg = start("start --only redis,postgres");
        let names: Vec<_> = cfg.services.iter().map(|s| s.name).collect();
        assert_eq!(names, ["postgres", "redis"]);
    }

    #[test]
    fn port_override() {
        let cfg = start("start --redis-port 6380");
        let redis = cfg.services.iter().find(|s| s.name == "redis").unwrap();
        assert_eq!(redis.port, 6380);
    }

    #[test]
    fn rejects_unknown_service_and_bad_port() {
        assert!(parse(&args("start --only redis,oracle")).is_err());
        assert!(parse(&args("start --redis-port nope")).is_err());
        assert!(parse(&args("start --oracle-port 1521")).is_err());
        assert!(parse(&args("start --data-dir")).is_err());
    }
}
