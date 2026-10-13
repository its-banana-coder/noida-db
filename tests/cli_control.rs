//! The runtime control commands against the real binary: `up` starts a
//! background server, `status`/`stop`/`start`/`reset`/`activity` drive it
//! through the control API, and `down` ends it. Each test has its own data
//! dir and free ports, and starts only the services it needs.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn noida(args: &[&str], dir: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_noida-db"))
        .args(args)
        .arg("--data-dir")
        .arg(dir)
        .env_remove("NO_COLOR")
        .output()
        .expect("run noida-db")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

/// Runs `args` and returns stdout, failing the test if the command fails.
fn ok(args: &[&str], dir: &Path) -> String {
    let out = noida(args, dir);
    assert!(
        out.status.success(),
        "noida-db {args:?} failed:\n{}{}",
        stdout(&out),
        String::from_utf8_lossy(&out.stderr)
    );
    stdout(&out)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn listening(port: u16) -> bool {
    TcpStream::connect(("127.0.0.1", port)).is_ok()
}

/// A background server for one test; `down` on drop, so a failing test
/// doesn't leave it running.
struct Server {
    dir: PathBuf,
    pid: u32,
}

impl Server {
    fn up(name: &str, args: &[&str]) -> Server {
        let dir = std::env::temp_dir().join(format!("noida-cli-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Server::start(dir, args)
    }

    /// `up` on `dir` as it is.
    fn start(dir: PathBuf, args: &[&str]) -> Server {
        let out = ok(&[&["up"], args].concat(), &dir);
        assert!(out.contains("running in the background"), "{out}");
        let control = std::fs::read_to_string(dir.join("noida.control.json")).unwrap();
        let control: serde_json::Value = serde_json::from_str(&control).unwrap();
        assert_eq!(control["token"].as_str().unwrap().len(), 32);
        Server { dir, pid: control["pid"].as_u64().unwrap() as u32 }
    }

    fn ok(&self, args: &[&str]) -> String {
        ok(args, &self.dir)
    }

    /// `down`, and checks the process really exited and cleaned up.
    fn down(&self) {
        let out = self.ok(&["down"]);
        assert!(out.contains("noida-db stopped"), "{out}");
        assert!(!self.dir.join("noida.control.json").exists());
        #[cfg(target_os = "linux")]
        {
            let deadline = Instant::now() + Duration::from_secs(5);
            let gone = || {
                std::fs::read_to_string(format!("/proc/{}/stat", self.pid))
                    .map_or(true, |s| s.split_whitespace().nth(2) == Some("Z"))
            };
            while !gone() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(gone(), "pid {} still running", self.pid);
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = noida(&["down"], &self.dir);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[cfg(feature = "redis")]
#[test]
fn redis_lifecycle_through_the_cli() {
    use redis::Commands;
    let port = free_port();
    let p = port.to_string();
    let server = Server::up("redis", &["--only", "redis", "--redis-port", &p]);
    let url = format!("redis://127.0.0.1:{port}");
    let connect = || redis::Client::open(format!("{url}/")).unwrap().get_connection();

    let status = server.ok(&["status"]);
    assert!(status.contains("✔ redis") && status.contains(&url), "{status}");
    assert!(status.contains("○ postgres"), "{status}");
    assert!(!status.contains('\x1b'), "no colors when piped: {status}");
    // `up` again just reports.
    assert!(server.ok(&["up"]).contains("already running"));

    let mut con = connect().unwrap();
    let _: () = con.set("greeting", "hello").unwrap();
    let _: () = con.rpush("list", "a").unwrap();

    // Stop: the port closes, the open connection is dropped, and nothing
    // connects any more.
    let out = server.ok(&["stop", "redis"]);
    assert!(out.contains("redis stopped"), "{out}");
    assert!(!listening(port));
    assert!(redis::cmd("PING").query::<String>(&mut con).is_err());
    assert!(connect().is_err());
    assert!(server.ok(&["status"]).contains("○ redis"));

    // Start: the data from before the stop is back.
    let out = server.ok(&["start", "redis"]);
    assert!(out.contains("redis running") && out.contains(&url), "{out}");
    let mut con = connect().unwrap();
    assert_eq!(con.get::<_, String>("greeting").unwrap(), "hello");
    let out = server.ok(&["restart", "redis"]);
    assert!(out.contains("redis running"), "{out}");
    let mut con = connect().unwrap();
    assert_eq!(con.lrange::<_, Vec<String>>("list", 0, -1).unwrap(), ["a"]);

    // Reset: still running, now empty. Without a TTY it needs --yes.
    let refused = noida(&["reset", "redis"], &server.dir);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("--yes"));
    let out = server.ok(&["reset", "redis", "--yes"]);
    assert!(out.contains("redis reset"), "{out}");
    let mut con = connect().unwrap();
    assert_eq!(con.get::<_, Option<String>>("greeting").unwrap(), None);
    assert!(!server.dir.join("redis.json").exists());

    // Activity shows the commands, without AUTH's password.
    let _ = redis::cmd("AUTH").arg("hunter2").query::<String>(&mut con);
    let activity = server.ok(&["activity"]);
    assert!(activity.contains("SET greeting hello"), "{activity}");
    assert!(activity.contains("RPUSH list a"), "{activity}");
    assert!(activity.contains("AUTH") && !activity.contains("hunter2"), "{activity}");
    assert!(server.ok(&["activity", "postgres"]).contains("no activity yet"));

    server.down();
    // Nothing running now: commands say so, and suggest `up`.
    let out = noida(&["status"], &server.dir);
    assert!(!out.status.success());
    assert!(stdout(&out).contains("isn't running") && stdout(&out).contains("noida-db up"));
    let out = noida(&["stop", "redis"], &server.dir);
    assert!(!out.status.success() && stdout(&out).contains("isn't running"));
    assert!(server.ok(&["down"]).contains("isn't running"));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_survives_stop_and_start() {
    let port = free_port();
    let p = port.to_string();
    let server = Server::up("postgres", &["--only", "postgres", "--postgres-port", &p]);
    let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
    assert!(server.ok(&["status"]).contains(&url));
    let connect = || postgres::Client::connect(&url, postgres::NoTls);

    let mut client = connect().unwrap();
    client.batch_execute("CREATE TABLE notes (id int, body text)").unwrap();
    client.execute("INSERT INTO notes VALUES ($1, $2)", &[&1i32, &"kept"]).unwrap();

    server.ok(&["stop", "postgres"]);
    assert!(!listening(port));
    assert!(connect().is_err());
    assert!(client.simple_query("SELECT 1").is_err());

    server.ok(&["start", "pg"]);
    let mut client = connect().unwrap();
    let row = client.query_one("SELECT body FROM notes WHERE id = 1", &[]).unwrap();
    assert_eq!(row.get::<_, String>(0), "kept");

    let activity = server.ok(&["activity"]);
    assert!(activity.contains("CREATE TABLE notes (id int, body text)"), "{activity}");
    assert!(activity.contains("INSERT INTO notes VALUES ($1, $2)"), "{activity}");

    // SIGTERM (what Ctrl-C and `docker stop` send) still saves on the way out.
    #[cfg(unix)]
    {
        client.execute("INSERT INTO notes VALUES ($1, $2)", &[&2i32, &"on exit"]).unwrap();
        drop(client);
        let pid = server.pid.to_string();
        assert!(Command::new("kill").args(["-TERM", &pid]).status().unwrap().success());
        let deadline = Instant::now() + Duration::from_secs(10);
        while server.dir.join("noida.control.json").exists() || listening(port) {
            assert!(Instant::now() < deadline, "still running after SIGTERM");
            std::thread::sleep(Duration::from_millis(20));
        }
        let again =
            Server::start(server.dir.clone(), &["--only", "postgres", "--postgres-port", &p]);
        let mut client = connect().unwrap();
        let n: i64 = client.query_one("SELECT count(*) FROM notes", &[]).unwrap().get(0);
        assert_eq!(n, 2);
        drop(client);
        again.down();
    }
    #[cfg(not(unix))]
    {
        drop(client);
        server.down();
    }
}

/// One HTTP request to Elasticsearch; the response body.
#[cfg(feature = "elasticsearch")]
fn es(port: u16, method: &str, path: &str, body: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port))?;
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut out = String::new();
    s.read_to_string(&mut out)?;
    Ok(out.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default())
}

#[cfg(feature = "elasticsearch")]
#[test]
fn elasticsearch_survives_stop_and_start() {
    let port = free_port();
    let p = port.to_string();
    let server = Server::up("es", &["--only", "elasticsearch", "--elasticsearch-port", &p]);
    assert!(server.ok(&["status"]).contains(&format!("http://127.0.0.1:{port}")));

    let put = es(port, "PUT", "/books/_doc/1?refresh=true", r#"{"title":"Dune"}"#).unwrap();
    assert!(put.contains("created"), "{put}");

    server.ok(&["stop", "elasticsearch"]);
    assert!(!listening(port));
    assert!(es(port, "GET", "/", "").is_err());

    server.ok(&["start", "es"]);
    let got = es(port, "GET", "/books/_doc/1", "").unwrap();
    assert!(got.contains(r#""found":true"#) && got.contains("Dune"), "{got}");

    // Reset while running: the index is gone.
    server.ok(&["reset", "es", "--yes"]);
    let got = es(port, "GET", "/books/_doc/1", "").unwrap();
    assert!(got.contains("index_not_found_exception"), "{got}");

    assert!(server.ok(&["activity"]).contains("PUT /books/_doc/1"));
    server.down();
}
