//! The commands that talk to a running server through its control API
//! (`up`, `down`, `status`, `start|stop|restart <service>`, `reset`,
//! `activity`), and their output: ✔/○ markers, connection strings ready to
//! paste, a hint for what to do next. Colors only on a terminal, and never
//! with NO_COLOR set.

use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use crate::config::{Command, Config};
use crate::control::Client;

/// `println!`, but a closed stdout (`noida-db activity -f | head`) ends
/// the command quietly instead of panicking.
macro_rules! say {
    () => { say!("") };
    ($($arg:tt)*) => {
        if writeln!(io::stdout(), $($arg)*).is_err() {
            std::process::exit(0);
        }
    };
}

/// How long `up` waits for the background server to answer.
const UP_WAIT: Duration = Duration::from_secs(30);

/// Runs every command but `start` (the server itself), `version` and `help`.
pub fn run(command: Command) -> ExitCode {
    let out = Style::stdout();
    let result = match command {
        Command::Up(cfg, args) => up(&out, &cfg, &args),
        Command::Down(dir) => down(&out, &dir),
        Command::Status(dir) => status(&out, &dir),
        Command::Control { action, services, data_dir } => {
            control(&out, action, &services, &data_dir)
        }
        Command::Reset { services, yes, data_dir } => reset(&out, &services, yes, &data_dir),
        Command::Activity { services, follow, data_dir } => {
            activity(&out, &services, follow, &data_dir)
        }
        Command::Start(_) | Command::Version | Command::Help => Ok(()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure(msg)) => {
            if !msg.is_empty() {
                let err = Style::stderr();
                eprintln!("{} {msg}", err.red("error:"));
            }
            ExitCode::FAILURE
        }
    }
}

/// A command that failed; its message (if any) goes to stderr.
struct Failure(String);

impl From<io::Error> for Failure {
    fn from(e: io::Error) -> Self {
        Failure(e.to_string())
    }
}

type Outcome = Result<(), Failure>;

/// The running server for `data_dir`, or a clear "it isn't running".
fn running(out: &Style, data_dir: &Path) -> Result<Client, Failure> {
    Client::find(data_dir).ok_or_else(|| {
        not_running(out, data_dir);
        Failure(String::new())
    })
}

fn not_running(out: &Style, data_dir: &Path) {
    say!("{} noida-db isn't running (data dir: {})", out.dim("○"), data_dir.display());
    say!("  start it in the background: {}", out.bold(&with_dir("noida-db up", data_dir)));
}

/// `cmd` with `--data-dir` added when it isn't the default, so a hint can
/// be pasted as is.
fn with_dir(cmd: &str, data_dir: &Path) -> String {
    if data_dir == Config::default().data_dir {
        cmd.to_string()
    } else {
        format!("{cmd} --data-dir {}", data_dir.display())
    }
}

fn up(out: &Style, cfg: &Config, args: &[String]) -> Outcome {
    let dir = &cfg.data_dir;
    if let Some(client) = Client::find(dir) {
        say!("{} noida-db is already running (pid {})", out.green("✔"), client.pid);
        return print_status(out, &client, dir);
    }
    std::fs::create_dir_all(dir)?;
    let log_path = dir.join("noida.log");
    let log = std::fs::File::create(&log_path)?;
    let mut cmd = std::process::Command::new(std::env::current_exe()?);
    cmd.arg("start")
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    detach(&mut cmd);
    let mut child = cmd.spawn()?;
    let deadline = Instant::now() + UP_WAIT;
    let client = loop {
        if let Some(c) = Client::find(dir).filter(|c| c.pid == child.id()) {
            break c;
        }
        let exited = child.try_wait()?;
        if exited.is_some() || Instant::now() >= deadline {
            if exited.is_none() {
                let _ = child.kill();
            }
            say!("{} noida-db didn't start. The end of {}:", out.red("✗"), log_path.display());
            let log = std::fs::read_to_string(&log_path).unwrap_or_default();
            let lines: Vec<&str> = log.lines().collect();
            for line in &lines[lines.len().saturating_sub(15)..] {
                say!("  {line}");
            }
            return Err(Failure(String::new()));
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    say!("{} noida-db is running in the background (pid {})", out.green("✔"), client.pid);
    print_status(out, &client, dir)
}

/// Starts `cmd` detached from this terminal: its own process group (so a
/// Ctrl-C here doesn't reach it) and, on Windows, no console.
fn detach(cmd: &mut std::process::Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
}

fn down(out: &Style, dir: &Path) -> Outcome {
    let Some(client) = Client::find(dir) else {
        say!("{} noida-db isn't running (data dir: {})", out.dim("○"), dir.display());
        return Ok(());
    };
    let _ = client.request("POST", "/v1/shutdown");
    let deadline = Instant::now() + Duration::from_secs(30);
    while client.alive() {
        if Instant::now() >= deadline {
            return Err(Failure(format!("noida-db (pid {}) is still running", client.pid)));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    say!("{} noida-db stopped; data saved in {}", out.green("✔"), dir.display());
    Ok(())
}

fn status(out: &Style, dir: &Path) -> Outcome {
    let client = running(out, dir)?;
    print_status(out, &client, dir)
}

fn print_status(out: &Style, client: &Client, dir: &Path) -> Outcome {
    let (_, st) = client.request("GET", "/v1/status")?;
    let mut line = format!(
        "noida-db {} · pid {} · up {}",
        st["version"].as_str().unwrap_or("?"),
        st["pid"],
        duration(st["uptime_ms"].as_u64().unwrap_or(0))
    );
    if let Some(rss) = st["rss_bytes"].as_u64() {
        line.push_str(&format!(" · {} RAM", bytes(rss)));
    }
    say!("{}", out.dim(&line));
    say!();
    let services = st["services"].as_array().cloned().unwrap_or_default();
    let mut missing = Vec::new();
    for s in &services {
        let name = s["name"].as_str().unwrap_or("?");
        match s["state"].as_str() {
            Some("running") => {
                let mut line = format!(
                    "  {} {}  {}",
                    out.green("✔"),
                    out.bold(&format!("{name:<13}")),
                    s["connect"].as_str().unwrap_or("")
                );
                if let Some(n) = s["data_bytes"].as_u64() {
                    line.push_str(&out.dim(&format!("  ({} saved)", bytes(n))));
                }
                say!("{line}");
            }
            Some("stopped") => {
                let (marker, why) = match s["error"].as_str() {
                    Some(e) => (out.red("✗"), e.to_string()),
                    None => (out.dim("○"), "stopped".to_string()),
                };
                let hint = with_dir(&format!("noida-db start {name}"), dir);
                say!(
                    "  {marker} {}  {why} {}",
                    out.bold(&format!("{name:<13}")),
                    out.dim(&format!("· start it: {hint}"))
                );
            }
            _ => missing.push(name),
        }
    }
    if !missing.is_empty() {
        say!("  {}", out.dim(&format!("not in this build: {}", missing.join(", "))));
    }
    say!();
    let data_dir = st["data_dir"].as_str().map(PathBuf::from).unwrap_or_else(|| dir.to_path_buf());
    say!(
        "  {}",
        out.dim(&format!(
            "data: {}  ·  log: {}",
            data_dir.display(),
            data_dir.join("noida.log").display()
        ))
    );
    let mut next =
        "next: noida-db activity -f  ·  noida-db stop <service>  ·  noida-db down".to_string();
    if dir != Config::default().data_dir {
        next.push_str(&format!("  (with --data-dir {})", dir.display()));
    }
    say!("  {}", out.dim(&next));
    Ok(())
}

fn control(out: &Style, action: &str, services: &[&str], dir: &Path) -> Outcome {
    let client = running(out, dir)?;
    let mut failed = false;
    for name in services {
        let (code, body) = client.request("POST", &format!("/v1/services/{name}/{action}"))?;
        let s = &body["service"];
        if code != 200 {
            let err = body["error"].as_str().unwrap_or("failed");
            say!("{} {name}: {err}", out.red("✗"));
            if err.contains("in use") {
                say!("  {}", out.dim(&format!("another server has that port; stop it, or pick one: noida-db up --{name}-port <n>")));
            }
            failed = true;
            continue;
        }
        match s["state"].as_str() {
            Some("running") => say!(
                "{} {} running  {}",
                out.green("✔"),
                out.bold(name),
                s["connect"].as_str().unwrap_or("")
            ),
            _ => say!(
                "{} {} stopped {}",
                out.dim("○"),
                out.bold(name),
                out.dim("(data saved, memory freed)")
            ),
        }
    }
    if failed { Err(Failure(String::new())) } else { Ok(()) }
}

fn reset(out: &Style, services: &[&'static str], yes: bool, dir: &Path) -> Outcome {
    let client = Client::find(dir);
    let names: Vec<&str> = if services.is_empty() {
        crate::config::DEFAULT_SERVICES
            .iter()
            .map(|s| s.name)
            .filter(|n| crate::services::available(n))
            .collect()
    } else {
        services.to_vec()
    };
    if !yes {
        let what = if services.is_empty() { "every service".to_string() } else { names.join(", ") };
        if !io::stdin().is_terminal() {
            return Err(Failure(format!(
                "reset deletes data; pass --yes to confirm (it would reset {what})"
            )));
        }
        print!(
            "Delete all saved data for {what} in {}? This can't be undone. [y/N] ",
            dir.display()
        );
        io::stdout().flush()?;
        let mut answer = String::new();
        io::stdin().lock().read_line(&mut answer)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes" | "YES" | "Yes") {
            say!("nothing deleted");
            return Ok(());
        }
    }
    let Some(client) = client else {
        // Nothing running holds the data in memory: delete the files.
        for name in names.iter().filter(|n| !crate::services::snapshot_files(n).is_empty()) {
            crate::services::delete_snapshots(name, dir)?;
            say!("{} {} reset {}", out.green("✔"), out.bold(name), out.dim("(saved data deleted)"));
        }
        return Ok(());
    };
    let mut failed = false;
    for name in &names {
        let (code, body) = client.request("POST", &format!("/v1/services/{name}/reset"))?;
        let s = &body["service"];
        if code != 200 {
            say!("{} {name}: {}", out.red("✗"), body["error"].as_str().unwrap_or("failed"));
            failed = true;
        } else if s["state"] == "running" {
            say!(
                "{} {} reset {}  {}",
                out.green("✔"),
                out.bold(name),
                out.dim("(empty, still running)"),
                s["connect"].as_str().unwrap_or("")
            );
        } else {
            say!(
                "{} {} reset {}",
                out.green("✔"),
                out.bold(name),
                out.dim("(saved data deleted; still stopped)")
            );
        }
    }
    if failed { Err(Failure(String::new())) } else { Ok(()) }
}

fn activity(out: &Style, services: &[&str], follow: bool, dir: &Path) -> Outcome {
    let client = running(out, dir)?;
    // The latest 200 first; when following, everything after that.
    let mut since = 0u64;
    let mut limit = 200;
    let mut shown = 0usize;
    loop {
        let path = format!("/v1/activity?since={since}&limit={limit}");
        let (code, body) = match client.request("GET", &path) {
            Ok(r) => r,
            Err(_) if follow && limit > 200 => {
                say!("{}", out.dim("noida-db stopped"));
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        if code != 200 {
            return Err(Failure(body["error"].as_str().unwrap_or("activity failed").to_string()));
        }
        for e in body["events"].as_array().into_iter().flatten() {
            let service = e["service"].as_str().unwrap_or("?");
            if !services.is_empty() && !services.contains(&service) {
                continue;
            }
            say!(
                "{}  {}  {}",
                out.dim(&clock(e["ts_ms"].as_u64().unwrap_or(0))),
                out.bold(&format!("{service:<13}")),
                e["text"].as_str().unwrap_or("")
            );
            shown += 1;
        }
        io::stdout().flush()?;
        since = body["next"].as_u64().unwrap_or(since);
        if !follow {
            if shown == 0 {
                say!("{} no activity yet", out.dim("○"));
                say!(
                    "  {}",
                    out.dim(&format!(
                        "run some queries, or watch as they happen: {}",
                        with_dir("noida-db activity -f", dir)
                    ))
                );
            }
            return Ok(());
        }
        if !client.alive() {
            say!("{}", out.dim("noida-db stopped"));
            return Ok(());
        }
        limit = 1000;
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// "3m 12s", "2h 5m", "45s".
pub fn duration(ms: u64) -> String {
    let s = ms / 1000;
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m {}s", s / 60, s % 60),
        3600..86400 => format!("{}h {}m", s / 3600, s % 3600 / 60),
        _ => format!("{}d {}h", s / 86400, s % 86400 / 3600),
    }
}

/// "512 B", "12 KB", "14.2 MB".
pub fn bytes(n: u64) -> String {
    match n {
        0..1024 => format!("{n} B"),
        1024..1_048_576 => format!("{} KB", n / 1024),
        1_048_576..1_073_741_824 => format!("{:.1} MB", n as f64 / 1_048_576.0),
        _ => format!("{:.1} GB", n as f64 / 1_073_741_824.0),
    }
}

/// The UTC time of day of `ts_ms`, "HH:MM:SS".
pub fn clock(ts_ms: u64) -> String {
    let s = ts_ms / 1000 % 86400;
    format!("{:02}:{:02}:{:02}", s / 3600, s % 3600 / 60, s % 60)
}

/// Whether to color, decided once per stream.
pub struct Style {
    color: bool,
}

impl Style {
    fn stdout() -> Self {
        Style { color: io::stdout().is_terminal() && !no_color() }
    }

    fn stderr() -> Self {
        Style { color: io::stderr().is_terminal() && !no_color() }
    }

    fn paint(&self, code: &str, s: &str) -> String {
        if self.color { format!("\x1b[{code}m{s}\x1b[0m") } else { s.to_string() }
    }

    fn green(&self, s: &str) -> String {
        self.paint("32", s)
    }

    fn red(&self, s: &str) -> String {
        self.paint("31", s)
    }

    fn dim(&self, s: &str) -> String {
        self.paint("2", s)
    }

    fn bold(&self, s: &str) -> String {
        self.paint("1", s)
    }
}

/// https://no-color.org: set and not empty.
fn no_color() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_units() {
        assert_eq!(duration(45_000), "45s");
        assert_eq!(duration(192_000), "3m 12s");
        assert_eq!(duration(7_500_000), "2h 5m");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(12 * 1024 + 5), "12 KB");
        assert_eq!(bytes(14_889_779), "14.2 MB");
        assert_eq!(clock(1_760_000_000_123), "08:53:20");
    }

    #[test]
    fn plain_style_has_no_escapes() {
        let s = Style { color: false };
        assert_eq!(s.green("✔"), "✔");
        let s = Style { color: true };
        assert_eq!(s.green("✔"), "\x1b[32m✔\x1b[0m");
    }

    #[test]
    fn hints_carry_a_non_default_data_dir() {
        assert_eq!(with_dir("noida-db up", Path::new(".noida-db")), "noida-db up");
        assert_eq!(with_dir("noida-db up", Path::new("/tmp/x")), "noida-db up --data-dir /tmp/x");
    }
}
