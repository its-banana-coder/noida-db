//! Command-line parsing. Hand-rolled to keep the binary small.

use std::path::PathBuf;

pub const USAGE: &str = "\
usage: noida-db <command> [options]

commands:
  up                     start in the background and print connection strings
  status                 show what's running and how to connect
  down                   stop the background server (data is saved)
  start                  run the server in the foreground (Ctrl-C stops it)
  start <service...>     start services on the running server
  stop <service...>      stop services (data is saved, memory freed)
  restart <service...>   stop, then start again
  reset <service...>     delete a service's data (--all: every service;
                         --yes: don't ask first)
  activity [-f]          show recent queries and commands (-f: keep following)
  version                print the version
  help                   print this message

start/up options:
  --data-dir <path>      where data lives (default: ./.noida-db)
  --host <addr>          address to bind (default: 127.0.0.1)
  --only <a,b,...>       enable only these services
  --<service>-port <n>   override a port, e.g. --redis-port 6380

Every command takes --data-dir, to pick which server it talks to.

services: postgres, mysql, redis, kafka, elasticsearch, clickhouse, mongodb";

#[derive(Debug, Clone, PartialEq)]
pub struct Service {
    pub name: &'static str,
    pub port: u16,
}

/// Every service noida-db speaks, on the port its real counterpart uses,
/// so existing clients work with their defaults.
pub const DEFAULT_SERVICES: [Service; 7] = [
    Service { name: "postgres", port: 5432 },
    Service { name: "mysql", port: 3306 },
    Service { name: "redis", port: 6379 },
    Service { name: "kafka", port: 9092 },
    Service { name: "elasticsearch", port: 9200 },
    Service { name: "clickhouse", port: 8123 },
    Service { name: "mongodb", port: 27017 },
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
            data_dir: PathBuf::from(".noida-db"),
            host: "127.0.0.1".into(),
            services: DEFAULT_SERVICES.to_vec(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    /// Run the server in the foreground.
    Start(Config),
    /// Run the server in the background; the options as given, for `start`.
    Up(Config, Vec<String>),
    Down(PathBuf),
    Status(PathBuf),
    /// `start`, `stop` or `restart` services of the running server.
    Control {
        action: &'static str,
        services: Vec<&'static str>,
        data_dir: PathBuf,
    },
    /// Empty `services` means all of them (`--all`).
    Reset {
        services: Vec<&'static str>,
        yes: bool,
        data_dir: PathBuf,
    },
    Activity {
        services: Vec<&'static str>,
        follow: bool,
        data_dir: PathBuf,
    },
    Version,
    Help,
}

pub fn parse(args: &[String]) -> Result<Command, String> {
    let Some((cmd, rest)) = args.split_first() else {
        return Ok(Command::Help);
    };
    match cmd.as_str() {
        // `start` alone (or with options) runs the server, as it always
        // has; `start redis` starts a service of the running one.
        "start" if !has_positional(rest) => parse_start(rest).map(Command::Start),
        "start" | "stop" | "restart" => {
            let action = if cmd == "start" {
                "start"
            } else if cmd == "stop" {
                "stop"
            } else {
                "restart"
            };
            let (services, data_dir, _) = parse_control(rest, &[])?;
            if services.is_empty() {
                let hint =
                    if action == "stop" { " (to stop everything: noida-db down)" } else { "" };
                return Err(format!(
                    "{action} needs a service, e.g. noida-db {action} redis{hint}"
                ));
            }
            Ok(Command::Control { action, services, data_dir })
        }
        "up" => Ok(Command::Up(parse_start(rest)?, rest.to_vec())),
        "down" => Ok(Command::Down(parse_control(rest, &[])?.1)).and_then(no_services(rest)),
        "status" => Ok(Command::Status(parse_control(rest, &[])?.1)).and_then(no_services(rest)),
        "reset" => {
            let (services, data_dir, flags) = parse_control(rest, &["--all", "--yes", "-y"])?;
            let all = flags.contains(&"--all");
            if services.is_empty() != all {
                return Err("reset needs a service (e.g. noida-db reset redis) or --all".into());
            }
            let yes = flags.contains(&"--yes") || flags.contains(&"-y");
            Ok(Command::Reset { services, yes, data_dir })
        }
        "activity" => {
            let (services, data_dir, flags) = parse_control(rest, &["-f", "--follow"])?;
            Ok(Command::Activity { services, follow: !flags.is_empty(), data_dir })
        }
        "version" | "--version" | "-V" => Ok(Command::Version),
        "help" | "--help" | "-h" => Ok(Command::Help),
        other => Err(format!("unknown command '{other}'")),
    }
}

/// The canonical name of a service, from its name or a common alias.
pub fn service_name(name: &str) -> Option<&'static str> {
    let name = match name {
        "pg" | "postgresql" => "postgres",
        "es" | "elastic" => "elasticsearch",
        "mongo" => "mongodb",
        n => n,
    };
    DEFAULT_SERVICES.iter().map(|s| s.name).find(|s| *s == name)
}

/// Whether `args` names something besides options and their values (every
/// `start` option takes a value).
fn has_positional(args: &[String]) -> bool {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if !a.starts_with('-') {
            return true;
        }
        it.next();
    }
    false
}

/// For a command that only takes `--data-dir`: rejects service names.
fn no_services(args: &[String]) -> impl FnOnce(Command) -> Result<Command, String> {
    let named = has_positional(args);
    move |cmd| if named { Err("this command doesn't take a service".into()) } else { Ok(cmd) }
}

/// Service names (space- or comma-separated), `--data-dir`, and any of the
/// value-less `flags` this command accepts.
fn parse_control(
    args: &[String],
    flags: &[&'static str],
) -> Result<(Vec<&'static str>, PathBuf, Vec<&'static str>), String> {
    let mut services = Vec::new();
    let mut data_dir = Config::default().data_dir;
    let mut seen = Vec::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == "--data-dir" {
            data_dir = PathBuf::from(it.next().ok_or("--data-dir needs a value")?);
        } else if let Some(flag) = flags.iter().find(|f| **f == arg) {
            seen.push(*flag);
        } else if arg.starts_with('-') {
            return Err(format!("unknown option '{arg}'"));
        } else {
            for name in arg.split(',').filter(|n| !n.is_empty()) {
                let service =
                    service_name(name).ok_or_else(|| format!("unknown service '{name}'"))?;
                if !services.contains(&service) {
                    services.push(service);
                }
            }
        }
    }
    Ok((services, data_dir, seen))
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

    fn dir() -> PathBuf {
        Config::default().data_dir
    }

    #[test]
    fn start_with_a_service_controls_the_running_server() {
        assert_eq!(
            parse(&args("start redis")).unwrap(),
            Command::Control { action: "start", services: vec!["redis"], data_dir: dir() }
        );
        assert_eq!(
            parse(&args("stop pg,redis --data-dir /tmp/d es")).unwrap(),
            Command::Control {
                action: "stop",
                services: vec!["postgres", "redis", "elasticsearch"],
                data_dir: "/tmp/d".into()
            }
        );
        assert!(matches!(
            parse(&args("restart kafka")).unwrap(),
            Command::Control { action: "restart", .. }
        ));
        // Options alone still run the server, values included.
        assert!(matches!(
            parse(&args("start --only redis --data-dir d")).unwrap(),
            Command::Start(_)
        ));
        assert!(parse(&args("stop")).unwrap_err().contains("noida-db down"));
        assert!(parse(&args("start oracle")).is_err());
        assert!(parse(&args("stop redis --host x")).is_err());
    }

    #[test]
    fn up_keeps_its_options_for_the_server() {
        let Command::Up(cfg, raw) = parse(&args("up --only redis --redis-port 0")).unwrap() else {
            panic!("expected up")
        };
        assert_eq!(cfg.services, [Service { name: "redis", port: 0 }]);
        assert_eq!(raw, args("--only redis --redis-port 0"));
        assert!(parse(&args("up redis")).is_err());
    }

    #[test]
    fn other_commands() {
        assert_eq!(parse(&args("status")).unwrap(), Command::Status(dir()));
        assert_eq!(parse(&args("down --data-dir x")).unwrap(), Command::Down("x".into()));
        assert!(parse(&args("status redis")).is_err());
        assert_eq!(
            parse(&args("reset redis -y")).unwrap(),
            Command::Reset { services: vec!["redis"], yes: true, data_dir: dir() }
        );
        assert_eq!(
            parse(&args("reset --all")).unwrap(),
            Command::Reset { services: vec![], yes: false, data_dir: dir() }
        );
        assert!(parse(&args("reset")).is_err());
        assert!(parse(&args("reset redis --all")).is_err());
        assert_eq!(
            parse(&args("activity -f")).unwrap(),
            Command::Activity { services: vec![], follow: true, data_dir: dir() }
        );
        assert_eq!(
            parse(&args("activity mongo")).unwrap(),
            Command::Activity { services: vec!["mongodb"], follow: false, data_dir: dir() }
        );
    }

    #[test]
    fn rejects_unknown_service_and_bad_port() {
        assert!(parse(&args("start --only redis,oracle")).is_err());
        assert!(parse(&args("start --redis-port nope")).is_err());
        assert!(parse(&args("start --oracle-port 1521")).is_err());
        assert!(parse(&args("start --data-dir")).is_err());
    }
}
