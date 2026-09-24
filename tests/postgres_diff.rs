//! Differential tests: every script runs against a real Postgres and against
//! noida, and the results must match — values, column names, column types and
//! SQLSTATE codes.
//!
//! The reference server is `NOIDA_POSTGRES_REF=host:port` if set (CI points
//! this at postgres:16), otherwise a local `initdb`/`postgres` pair started in
//! a temporary directory. With neither, the test prints SKIPPED and passes.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};

/// Scripts to compare. `@16` marks a statement that only matches on
/// Postgres 16 or newer, `!` one that runs on both without comparing.
const SCRIPTS: &[&[&str]] = &[
    // Literals, arithmetic and types.
    &[
        "SELECT 1, -1, 2147483647, 2147483648, 9223372036854775807",
        "SELECT 1 + 2, 7 / 2, -7 / 2, 7 % 3, -7 % 3, 2 * 3",
        "SELECT 1.5, 1.50, 0.1 + 0.2, 1/3.0, 10/4.0, 2::numeric/3",
        "SELECT 1e20::float8, 1e15::float8, 0.00001::float8, 100.0::float8, 0.1::float4",
        "SELECT 'a' || 'b', 'a' || 1, length('hello'), upper('aBc'), lower('AbC')",
        "SELECT true, false, NULL, true AND NULL, false AND NULL, true OR NULL",
        "SELECT pg_typeof(1), pg_typeof(1.5), pg_typeof('a'), pg_typeof(1::int8), pg_typeof(now())",
        "SELECT 1 = 1, 1 <> 2, 'a' < 'b', NULL = NULL, NULL IS NULL, 1 IS DISTINCT FROM NULL",
    ],
    &[
        "SELECT 2147483647 + 1",
        "SELECT 1/0",
        "SELECT 'abc'::int",
        "SELECT 1.5::int, 2.5::int, -1.5::int, 2.5::float8::int, 3.5::float8::int",
        "SELECT 32767::int2 + 1",
        "SELECT 'x'::bool",
        "SELECT 'nope'",
        "SELECT nosuchfunction(1)",
        "SELECT 1 + 'a'",
    ],
    // Strings.
    &[
        "SELECT substr('hello world', 1, 5), substring('hello' from 2 for 3), left('abc', 2), right('abc', 2)",
        "SELECT trim('  x  '), btrim('xxaxx', 'x'), ltrim('xxa', 'x'), rtrim('axx', 'x')",
        "SELECT lpad('7', 3, '0'), rpad('7', 3, '0'), repeat('ab', 3), reverse('abc')",
        "SELECT replace('a-b-c', '-', '+'), split_part('a,b,c', ',', 2), strpos('hello', 'll')",
        "SELECT concat('a', NULL, 1), concat_ws('-', 'a', NULL, 'b'), format('%s/%I/%L', 'a', 'b c', 'd')",
        "SELECT md5('abc'), encode('abc'::bytea, 'hex'), encode('abc'::bytea, 'base64'), decode('616263', 'hex')",
        "SELECT 'Hello' LIKE 'H%', 'Hello' LIKE '_ello', 'Hello' ILIKE 'h%', 'a%b' LIKE 'a\\%b'",
        "SELECT 'abc' ~ '^a', 'abc' ~* 'B', 'abc' !~ 'd', regexp_replace('a1b2', '[0-9]', 'x', 'g')",
        "SELECT initcap('hello world'), ascii('A'), chr(66), to_hex(255)",
        "SELECT 'ab'::char(5) || '|', length('ab'::char(5)), 'abc'::varchar(2)",
        "SELECT 'abc'::varchar(2) || 'x'",
        "SELECT string_to_array('a,b,c', ','), array_to_string(ARRAY['a','b'], '-')",
    ],
    // Dates and times (fixed zone so results are deterministic).
    &[
        "SET TimeZone = 'UTC'",
        "SELECT date '2020-02-29', timestamp '2020-01-01 10:20:30.5', timestamptz '2020-01-01 10:00:00+05:30'",
        "SELECT interval '1 day', interval '-1 days 2 hours', interval '1.5 days', interval 'P1Y2M3DT4H5M6S'",
        "SELECT date '2020-01-01' + 30, date '2020-03-01' - date '2020-01-01', date '2020-01-31' + interval '1 month'",
        "SELECT timestamp '2020-01-02 02:00' - timestamp '2020-01-01', age(timestamp '2021-03-15', timestamp '2020-01-20')",
        "SELECT extract(year from date '2020-05-06'), extract(dow from date '2020-05-06'), extract(epoch from timestamptz '2020-01-01 00:00Z')",
        "SELECT date_trunc('month', timestamp '2020-05-06 07:08:09'), date_trunc('hour', timestamp '2020-05-06 07:08:09')",
        "SELECT to_char(timestamp '2020-03-05 14:07:09', 'YYYY-MM-DD HH24:MI:SS'), to_char(date '2020-03-05', 'FMMonth DD, YYYY')",
        "SELECT make_date(2020,2,29), make_interval(days => 3), justify_hours(interval '36 hours')",
        "SELECT timestamp '2020-06-01 12:00' AT TIME ZONE 'UTC'",
        "SELECT date '2021-02-29'",
        "SELECT extract(hour from date '2020-01-01')",
    ],
    // DDL, DML and constraints.
    &[
        "CREATE TABLE t (id serial PRIMARY KEY, name text NOT NULL, email varchar(64) UNIQUE, n int DEFAULT 7)",
        "INSERT INTO t (name, email) VALUES ('ann', 'a@x'), ('bob', 'b@x')",
        "SELECT id, name, email, n FROM t ORDER BY id",
        "INSERT INTO t (name, email) VALUES ('cid', 'a@x')",
        "INSERT INTO t (email) VALUES ('c@x')",
        "UPDATE t SET n = n + 1 WHERE name = 'ann'",
        "SELECT name, n FROM t ORDER BY name",
        "DELETE FROM t WHERE name = 'bob'",
        "SELECT count(*) FROM t",
        "INSERT INTO t (name) VALUES ('dee') RETURNING id, n",
        "SELECT currval('t_id_seq'), nextval('t_id_seq')",
        "DROP TABLE t",
        "SELECT * FROM t",
    ],
    &[
        "CREATE TABLE p (id int PRIMARY KEY, v int CHECK (v > 0))",
        "CREATE TABLE c (id int PRIMARY KEY, p_id int REFERENCES p(id) ON DELETE CASCADE)",
        "INSERT INTO p VALUES (1, 5), (2, 10)",
        "INSERT INTO p VALUES (3, -1)",
        "INSERT INTO c VALUES (1, 1), (2, 1)",
        "INSERT INTO c VALUES (3, 99)",
        "DELETE FROM p WHERE id = 1",
        "SELECT * FROM c ORDER BY id",
        "ALTER TABLE p ADD COLUMN w int DEFAULT 3",
        "SELECT id, v, w FROM p ORDER BY id",
        "ALTER TABLE p DROP COLUMN w",
        "ALTER TABLE p RENAME COLUMN v TO val",
        "SELECT id, val FROM p ORDER BY id",
        "ALTER TABLE p ALTER COLUMN val TYPE bigint",
        "SELECT pg_typeof(val) FROM p LIMIT 1",
    ],
    // Queries: joins, grouping, ordering, subqueries, CTEs, windows.
    &[
        "CREATE TABLE a (id int, name text)",
        "CREATE TABLE b (id int, a_id int, v numeric)",
        "INSERT INTO a VALUES (1,'ann'),(2,'bob'),(3,'cid')",
        "INSERT INTO b VALUES (1,1,10.5),(2,1,20),(3,2,5.25)",
        "SELECT a.name, b.v FROM a JOIN b ON b.a_id = a.id ORDER BY a.name, b.v",
        "SELECT a.name, b.v FROM a LEFT JOIN b ON b.a_id = a.id ORDER BY a.name, b.v",
        "SELECT a.name, b.v FROM b RIGHT JOIN a ON b.a_id = a.id ORDER BY a.name, b.v",
        "SELECT a.name, count(b.id), sum(b.v), avg(b.v), min(b.v), max(b.v) FROM a LEFT JOIN b ON b.a_id=a.id GROUP BY a.name ORDER BY a.name",
        "SELECT count(*), count(v), sum(v), avg(v) FROM b",
        "SELECT a_id, count(*) FROM b GROUP BY a_id HAVING count(*) > 1 ORDER BY a_id",
        "SELECT name FROM a WHERE id IN (SELECT a_id FROM b) ORDER BY name",
        "SELECT name, (SELECT count(*) FROM b WHERE b.a_id = a.id) AS n FROM a ORDER BY name",
        "SELECT name FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id) ORDER BY name",
        "WITH x AS (SELECT a_id, sum(v) s FROM b GROUP BY a_id) SELECT a.name, x.s FROM a JOIN x ON x.a_id = a.id ORDER BY a.name",
        "SELECT id, name FROM a ORDER BY name DESC LIMIT 2 OFFSET 1",
        "SELECT DISTINCT a_id FROM b ORDER BY a_id",
        "SELECT a_id, v, row_number() OVER (PARTITION BY a_id ORDER BY v) FROM b ORDER BY a_id, v",
        "SELECT a_id, sum(v) OVER (ORDER BY a_id) FROM b ORDER BY a_id",
        "SELECT id FROM a UNION SELECT a_id FROM b ORDER BY 1",
        "SELECT id FROM a EXCEPT SELECT a_id FROM b ORDER BY 1",
        "SELECT id FROM a INTERSECT SELECT a_id FROM b ORDER BY 1",
        "SELECT name, count(*) FROM a GROUP BY 1 ORDER BY 1",
        "SELECT v FROM b ORDER BY v DESC NULLS LAST",
        "SELECT a.name FROM a GROUP BY a.id ORDER BY a.name",
        "SELECT name, id FROM a GROUP BY name",
    ],
    // JSON and arrays.
    &[
        "SELECT '{\"a\": 1, \"b\": [1,2]}'::jsonb, '{\"b\":1,\"a\":2,\"b\":3}'::jsonb",
        "SELECT '{\"a\": {\"b\": 2}}'::jsonb -> 'a' -> 'b', '{\"a\": {\"b\": 2}}'::jsonb ->> 'a'",
        "SELECT '{\"a\": 1}'::jsonb @> '{\"a\": 1}', '{\"a\": 1}'::jsonb ? 'a', jsonb_typeof('[]'::jsonb)",
        "SELECT jsonb_build_object('a', 1, 'b', 'x'), json_build_object('a', 1), jsonb_build_array(1, 'a')",
        "SELECT jsonb_array_length('[1,2,3]'::jsonb), jsonb_set('{\"a\":1}'::jsonb, '{a}', '2'), jsonb_pretty('{\"a\":1}'::jsonb)",
        "SELECT to_jsonb(1), to_jsonb('a'::text), to_json(ARRAY[1,2]), row_to_json(row(1,'a'))",
        "SELECT '\"x\"'::jsonb, '1'::jsonb, 'null'::jsonb, '{bad}'::jsonb",
        "SELECT ARRAY[1,2,3], ARRAY['a','b'], ARRAY[[1,2],[3,4]], '{}'::int[]",
        "SELECT array_length(ARRAY[1,2,3], 1), cardinality(ARRAY[1,2]), array_append(ARRAY[1], 2), array_cat(ARRAY[1], ARRAY[2])",
        "@16 SELECT ARRAY[1,2,3][2], (ARRAY[1,2,3])[1:2], 2 = ANY(ARRAY[1,2]), 5 = ALL(ARRAY[5,5])",
        "SELECT (ARRAY[1,2,3])[2], (ARRAY[1,2,3])[1:2], 2 = ANY(ARRAY[1,2]), 5 = ALL(ARRAY[5,5])",
        "SELECT unnest(ARRAY[1,2,3])",
        "SELECT * FROM generate_series(1, 5, 2)",
        "SELECT generate_series(1,3) AS g, 'x'",
    ],
    // Transactions.
    &[
        "CREATE TABLE t (id int)",
        "BEGIN",
        "INSERT INTO t VALUES (1)",
        "SAVEPOINT s1",
        "INSERT INTO t VALUES (2)",
        "ROLLBACK TO SAVEPOINT s1",
        "INSERT INTO t VALUES (3)",
        "SELECT * FROM t ORDER BY id",
        "COMMIT",
        "SELECT * FROM t ORDER BY id",
        "BEGIN",
        "INSERT INTO t VALUES (4)",
        "ROLLBACK",
        "SELECT count(*) FROM t",
        "COMMIT",
    ],
    // ON CONFLICT and generated columns.
    &[
        "CREATE TABLE t (id int PRIMARY KEY, n int, s text GENERATED ALWAYS AS (n::text) STORED)",
        "INSERT INTO t (id, n) VALUES (1, 1)",
        "INSERT INTO t (id, n) VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = excluded.n + t.n",
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t (id, n) VALUES (1, 9) ON CONFLICT DO NOTHING",
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t (id, n) VALUES (2, 2) ON CONFLICT (id) DO UPDATE SET n = 0 RETURNING id, n",
    ],
    // Catalog queries drivers and ORMs send.
    &[
        "CREATE TABLE t (id serial PRIMARY KEY, name varchar(20) NOT NULL, data jsonb, ts timestamptz)",
        "CREATE INDEX t_name_idx ON t (name)",
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
        "SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position",
        "SELECT relname, relkind, relnatts FROM pg_class WHERE relname = 't'",
        "SELECT attname, atttypid::regtype::text, attnotnull, attnum FROM pg_attribute WHERE attrelid = 't'::regclass AND attnum > 0 ORDER BY attnum",
        "SELECT conname, contype FROM pg_constraint WHERE conrelid = 't'::regclass ORDER BY conname",
        "SELECT indexname FROM pg_indexes WHERE tablename = 't' ORDER BY indexname",
        "SELECT nspname FROM pg_namespace WHERE nspname IN ('public','pg_catalog','information_schema') ORDER BY nspname",
        "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 't' AND constraint_type <> 'CHECK' ORDER BY constraint_name",
        "SELECT typname, typtype FROM pg_type WHERE typname IN ('int4','text','jsonb','varchar') ORDER BY typname",
        "SELECT current_schema(), current_database()",
        "SELECT format_type('int4'::regtype, -1), format_type('varchar'::regtype, 24), format_type('numeric'::regtype, 655366)",
        "SELECT 't'::regclass::oid = (SELECT oid FROM pg_class WHERE relname='t')",
        "SELECT has_table_privilege('t', 'SELECT'), pg_table_is_visible('t'::regclass)",
    ],
    // Views and schemas.
    &[
        "CREATE SCHEMA s",
        "CREATE TABLE s.t (id int, v text)",
        "INSERT INTO s.t VALUES (1, 'a'), (2, 'b')",
        "CREATE VIEW v AS SELECT id, v FROM s.t WHERE id > 1",
        "SELECT * FROM v",
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name IN ('t','v') ORDER BY table_schema, table_name",
        "SET search_path = s, public",
        "SELECT count(*) FROM t",
        "RESET search_path",
        "DROP VIEW v",
        "DROP SCHEMA s CASCADE",
        "SELECT count(*) FROM s.t",
    ],
];

fn main_test_body() {}

struct Ref {
    client: Client,
    version: u32,
    _server: Option<Server>,
}

struct Server {
    child: Child,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Connects to the reference server named by `NOIDA_POSTGRES_REF`, or starts
/// a local one with initdb.
fn reference() -> Option<Ref> {
    if let Ok(hostport) = std::env::var("NOIDA_POSTGRES_REF") {
        let (host, port) = hostport.split_once(':').unwrap_or((hostport.as_str(), "5432"));
        let user = std::env::var("NOIDA_POSTGRES_REF_USER").unwrap_or_else(|_| "postgres".into());
        let password =
            std::env::var("NOIDA_POSTGRES_REF_PASSWORD").unwrap_or_else(|_| "postgres".into());
        let url =
            format!("host={host} port={port} user={user} password={password} dbname=postgres");
        let client = wait_for(&url)?;
        let version = server_version(&url)?;
        return Some(Ref { client, version, _server: None });
    }
    let bin =
        ["/usr/lib/postgresql/16/bin", "/usr/lib/postgresql/15/bin", "/usr/lib/postgresql/14/bin"]
            .into_iter()
            .map(Path::new)
            .find(|p| p.join("initdb").exists())?;
    let dir = std::env::temp_dir().join(format!("noida-pgref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let data = dir.join("data");
    std::fs::create_dir_all(&data).ok()?;
    let ok = Command::new(bin.join("initdb"))
        .args([
            "-D",
            data.to_str()?,
            "-A",
            "trust",
            "-U",
            "postgres",
            "--no-sync",
            "--locale=C",
            "-E",
            "UTF8",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?
        .success();
    if !ok {
        return None;
    }
    let port = free_port();
    let child = Command::new(bin.join("postgres"))
        .args([
            "-D",
            data.to_str()?,
            "-p",
            &port.to_string(),
            "-k",
            dir.to_str()?,
            "-c",
            "listen_addresses=127.0.0.1",
            "-c",
            "timezone=UTC",
            "-c",
            "fsync=off",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let server = Server { child, dir };
    let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
    let client = wait_for(&url)?;
    let version = server_version(&url)?;
    Some(Ref { client, version, _server: Some(server) })
}

fn wait_for(url: &str) -> Option<Client> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match Client::connect(url, NoTls) {
            Ok(c) => return Some(c),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(_) => return None,
        }
    }
}

fn server_version(url: &str) -> Option<u32> {
    let mut c = Client::connect(url, NoTls).ok()?;
    let row = c.query_one("SHOW server_version_num", &[]).ok()?;
    row.get::<_, &str>(0).parse().ok()
}

/// One statement's observable result.
#[derive(PartialEq, Debug)]
enum Outcome {
    /// Column names, then rows of text values.
    Rows(Vec<String>, Vec<Vec<Option<String>>>),
    Tag(String),
    Error(String),
}

fn run(client: &mut Client, sql: &str) -> Outcome {
    match client.simple_query(sql) {
        Err(e) => match e.as_db_error() {
            Some(db) => Outcome::Error(db.code().code().to_string()),
            None => Outcome::Error(format!("connection: {e}")),
        },
        Ok(messages) => {
            let mut names = vec![];
            let mut rows = vec![];
            let mut tag = None;
            for m in messages {
                match m {
                    SimpleQueryMessage::Row(r) => {
                        if names.is_empty() {
                            names = r.columns().iter().map(|c| c.name().to_string()).collect();
                        }
                        rows.push((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect());
                    }
                    SimpleQueryMessage::RowDescription(cols) => {
                        names = cols.iter().map(|c| c.name().to_string()).collect();
                    }
                    SimpleQueryMessage::CommandComplete(n) => {
                        tag = Some(n.to_string());
                    }
                    _ => {}
                }
            }
            if names.is_empty() {
                Outcome::Tag(tag.map(|t| t.to_string()).unwrap_or_default())
            } else {
                Outcome::Rows(names, rows)
            }
        }
    }
}

/// Column types as the extended protocol reports them (skipped for
/// statements the driver cannot prepare).
fn describe(client: &mut Client, sql: &str) -> Option<Vec<String>> {
    if !sql.trim_start().to_lowercase().starts_with("select") {
        return None;
    }
    let stmt = client.prepare(sql).ok()?;
    Some(stmt.columns().iter().map(|c| c.type_().name().to_string()).collect())
}

#[test]
fn differential() {
    main_test_body();
    let Some(mut reference) = reference() else {
        println!("SKIPPED: no reference Postgres (set NOIDA_POSTGRES_REF or install postgresql)");
        return;
    };
    let addr = noida::postgres::spawn("127.0.0.1:0").expect("start noida");
    let noida_url = format!("host=127.0.0.1 port={} user=postgres dbname=postgres", addr.port());

    let mut compared = 0usize;
    let mut skipped = 0usize;
    let mut failures = vec![];
    for (i, script) in SCRIPTS.iter().enumerate() {
        // Each script gets a clean schema on both servers.
        let mut mine = Client::connect(&noida_url, NoTls).expect("connect to noida");
        reset(&mut reference.client);
        reset(&mut mine);
        for raw in script.iter() {
            let (sql, compare, min_version) = directives(raw);
            let want = run(&mut reference.client, sql);
            let got = run(&mut mine, sql);
            if !compare || reference.version < min_version {
                skipped += 1;
                continue;
            }
            compared += 1;
            if want != got {
                failures
                    .push(format!("script {i}: {sql}\n  postgres: {want:?}\n  noida:    {got:?}"));
                continue;
            }
            // Result column types must match too.
            if let Some(want_types) = describe(&mut reference.client, sql) {
                let got_types = describe(&mut mine, sql);
                compared += 1;
                if got_types.as_ref() != Some(&want_types) {
                    failures.push(format!(
                        "script {i} column types: {sql}\n  postgres: {want_types:?}\n  noida:    {got_types:?}"
                    ));
                }
            }
        }
    }
    println!(
        "compared {compared} results against PostgreSQL {} ({skipped} skipped)",
        reference.version
    );
    if !failures.is_empty() {
        let shown: Vec<String> = failures.iter().take(40).cloned().collect();
        panic!("{} differences:\n{}", failures.len(), shown.join("\n"));
    }
}

fn reset(client: &mut Client) {
    let _ = client.simple_query("ROLLBACK");
    let _ = client.simple_query(
        "DROP SCHEMA IF EXISTS s CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; RESET ALL",
    );
}

/// Strips `!` (run but don't compare) and `@NN` (minimum server version).
fn directives(raw: &str) -> (&str, bool, u32) {
    let mut sql = raw;
    let mut compare = true;
    let mut min_version = 0;
    loop {
        if let Some(rest) = sql.strip_prefix('!') {
            compare = false;
            sql = rest.trim_start();
            continue;
        }
        if let Some(rest) = sql.strip_prefix('@') {
            let (v, rest) = rest.split_once(' ').unwrap_or((rest, ""));
            min_version = v.parse::<u32>().unwrap_or(0) * 10_000;
            sql = rest.trim_start();
            continue;
        }
        break;
    }
    (sql, compare, min_version)
}
