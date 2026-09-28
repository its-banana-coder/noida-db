//! Real-client tests: the `postgres` crate talks to noida-db over TCP,
//! exercising the simple and extended query protocols.

use std::net::SocketAddr;

use postgres::types::{ToSql, Type};
use postgres::{Client, NoTls, SimpleQueryMessage};

fn start() -> SocketAddr {
    noida::postgres::spawn("127.0.0.1:0").expect("start noida-db postgres")
}

fn connect(addr: SocketAddr) -> Client {
    let url = format!("host=127.0.0.1 port={} user=postgres dbname=postgres", addr.port());
    Client::connect(&url, NoTls).expect("connect")
}

fn client() -> Client {
    connect(start())
}

#[test]
fn connects_and_selects() {
    let mut c = client();
    let row = c.query_one("SELECT 1 AS a, 'hi' AS b, 2 + 3 * 4 AS c", &[]).unwrap();
    assert_eq!(row.get::<_, i32>("a"), 1);
    assert_eq!(row.get::<_, &str>("b"), "hi");
    assert_eq!(row.get::<_, i32>("c"), 14);
}

#[test]
fn extended_query_with_parameters() {
    let mut c = client();
    c.execute("CREATE TABLE t (id int primary key, name text, weight float8)", &[]).unwrap();
    let n = c.execute("INSERT INTO t VALUES ($1, $2, $3)", &[&1i32, &"ann", &1.5f64]).unwrap();
    assert_eq!(n, 1);
    c.execute("INSERT INTO t VALUES ($1, $2, $3)", &[&2i32, &"bob", &2.25f64]).unwrap();
    let rows =
        c.query("SELECT id, name, weight FROM t WHERE id > $1 ORDER BY id", &[&0i32]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, &str>(1), "bob");
    assert_eq!(rows[1].get::<_, f64>(2), 2.25);
    // Parameter types are inferred from context, so the driver can send binary.
    let stmt = c.prepare("SELECT name FROM t WHERE id = $1").unwrap();
    assert_eq!(stmt.params()[0], Type::INT4);
    assert_eq!(stmt.columns()[0].type_(), &Type::TEXT);
    let row = c.query_one(&stmt, &[&2i32]).unwrap();
    assert_eq!(row.get::<_, &str>(0), "bob");
}

#[test]
fn round_trips_common_types() {
    let mut c = client();
    let row = c
        .query_one(
            "SELECT $1::int2, $2::int4, $3::int8, $4::float4, $5::float8, $6::bool, $7::text, $8::bytea",
            &[&1i16, &2i32, &3i64, &1.5f32, &2.5f64, &true, &"x", &vec![1u8, 2, 255]],
        )
        .unwrap();
    assert_eq!(row.get::<_, i16>(0), 1);
    assert_eq!(row.get::<_, i32>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 3);
    assert_eq!(row.get::<_, f32>(3), 1.5);
    assert_eq!(row.get::<_, f64>(4), 2.5);
    assert!(row.get::<_, bool>(5));
    assert_eq!(row.get::<_, &str>(6), "x");
    assert_eq!(row.get::<_, Vec<u8>>(7), vec![1u8, 2, 255]);
}

#[test]
fn round_trips_json_uuid_and_time() {
    let mut c = client();
    let json: serde_json::Value = serde_json::json!({"a": [1, 2], "b": "x"});
    let id = uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
    let date = chrono::NaiveDate::from_ymd_opt(2020, 2, 29).unwrap();
    let ts = date.and_hms_micro_opt(10, 20, 30, 123_456).unwrap();
    let row = c
        .query_one("SELECT $1::jsonb, $2::uuid, $3::date, $4::timestamp", &[&json, &id, &date, &ts])
        .unwrap();
    assert_eq!(row.get::<_, serde_json::Value>(0), json);
    assert_eq!(row.get::<_, uuid::Uuid>(1), id);
    assert_eq!(row.get::<_, chrono::NaiveDate>(2), date);
    assert_eq!(row.get::<_, chrono::NaiveDateTime>(3), ts);
}

#[test]
fn arrays_and_nulls() {
    let mut c = client();
    let v: Vec<i32> = vec![1, 2, 3];
    let row = c.query_one("SELECT $1::int[] AS a, NULL::text AS b", &[&v]).unwrap();
    assert_eq!(row.get::<_, Vec<i32>>("a"), v);
    assert_eq!(row.get::<_, Option<String>>("b"), None);
    let row = c.query_one("SELECT ARRAY['a','b']::text[] AS a", &[]).unwrap();
    assert_eq!(row.get::<_, Vec<String>>("a"), vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn numeric_as_string() {
    let mut c = client();
    let rows = c.simple_query("SELECT 1/3.0 AS a, 10/4.0 AS b, 2.5::numeric(4,1) AS c").unwrap();
    let row = match &rows[1] {
        SimpleQueryMessage::Row(r) => r,
        _ => panic!("expected a row"),
    };
    assert_eq!(row.get("a"), Some("0.33333333333333333333"));
    assert_eq!(row.get("b"), Some("2.5000000000000000"));
    assert_eq!(row.get("c"), Some("2.5"));
}

#[test]
fn transactions_and_savepoints() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int)").unwrap();
    let mut tx = c.transaction().unwrap();
    tx.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    let mut sp = tx.savepoint("s1").unwrap();
    sp.execute("INSERT INTO t VALUES (2)", &[]).unwrap();
    sp.rollback().unwrap();
    tx.execute("INSERT INTO t VALUES (3)", &[]).unwrap();
    tx.commit().unwrap();
    let rows = c.query("SELECT id FROM t ORDER BY id", &[]).unwrap();
    let ids: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![1, 3]);
    // A rolled back transaction leaves nothing behind.
    let mut tx = c.transaction().unwrap();
    tx.execute("INSERT INTO t VALUES (9)", &[]).unwrap();
    drop(tx);
    assert_eq!(c.query("SELECT count(*) FROM t", &[]).unwrap()[0].get::<_, i64>(0), 2);
}

#[test]
fn errors_carry_sqlstate_and_fields() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int primary key, name text not null)").unwrap();
    c.execute("INSERT INTO t VALUES (1, 'a')", &[]).unwrap();
    let e = c.execute("INSERT INTO t VALUES (1, 'b')", &[]).unwrap_err();
    let db = e.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "23505");
    assert_eq!(db.constraint(), Some("t_pkey"));
    assert!(db.detail().unwrap().contains("already exists"));

    let e = c.execute("INSERT INTO t VALUES (2, NULL)", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "23502");

    let e = c.query("SELECT * FROM nope", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "42P01");

    let e = c.query("SELECT nope FROM t", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "42703");

    let e = c.query("SELECT 1 +", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "42601");

    // The session stays usable after an error.
    assert_eq!(c.query_one("SELECT 1", &[]).unwrap().get::<_, i32>(0), 1);
}

#[test]
fn failed_transaction_blocks_until_rollback() {
    let mut c = client();
    c.batch_execute("BEGIN").unwrap();
    let e = c.query("SELECT * FROM nope", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "42P01");
    let e = c.query("SELECT 1", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "25P02");
    c.batch_execute("ROLLBACK").unwrap();
    assert_eq!(c.query_one("SELECT 1", &[]).unwrap().get::<_, i32>(0), 1);
}

#[test]
fn simple_query_runs_several_statements() {
    let mut c = client();
    let messages = c
        .simple_query(
            "CREATE TABLE t (id int); INSERT INTO t VALUES (1),(2); SELECT count(*) FROM t",
        )
        .unwrap();
    let tags: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(tags, vec!["0".to_string(), "2".to_string(), "1".to_string()]);
}

#[test]
fn returning_and_defaults() {
    let mut c = client();
    c.batch_execute(
        "CREATE TABLE t (id serial primary key, name text, created timestamptz default now(), n int default 7)",
    )
    .unwrap();
    let rows = c.query("INSERT INTO t (name) VALUES ('a'), ('b') RETURNING id, n", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[1].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, i32>("n"), 7);
    let row = c.query_one("UPDATE t SET n = n + 1 WHERE id = 1 RETURNING n", &[]).unwrap();
    assert_eq!(row.get::<_, i32>(0), 8);
    let row = c.query_one("DELETE FROM t WHERE id = 2 RETURNING name", &[]).unwrap();
    assert_eq!(row.get::<_, &str>(0), "b");
}

#[test]
fn on_conflict_upsert() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int primary key, n int)").unwrap();
    c.execute("INSERT INTO t VALUES (1, 1)", &[]).unwrap();
    c.execute(
        "INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = excluded.n + t.n",
        &[],
    )
    .unwrap();
    assert_eq!(c.query_one("SELECT n FROM t", &[]).unwrap().get::<_, i32>(0), 6);
    c.execute("INSERT INTO t VALUES (1, 9) ON CONFLICT DO NOTHING", &[]).unwrap();
    assert_eq!(c.query_one("SELECT n FROM t", &[]).unwrap().get::<_, i32>(0), 6);
}

#[test]
fn joins_grouping_and_subqueries() {
    let mut c = client();
    c.batch_execute(
        "CREATE TABLE authors (id int primary key, name text);
         CREATE TABLE books (id int primary key, author_id int references authors(id), title text, price numeric);
         INSERT INTO authors VALUES (1,'ann'),(2,'bob'),(3,'cid');
         INSERT INTO books VALUES (1,1,'a',10.5),(2,1,'b',20.0),(3,2,'c',5.25);",
    )
    .unwrap();
    let rows = c
        .query(
            "SELECT a.name, count(b.id) AS n, coalesce(sum(b.price), 0) AS total
             FROM authors a LEFT JOIN books b ON b.author_id = a.id
             GROUP BY a.name HAVING count(b.id) >= 0 ORDER BY a.name",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, &str>("name"), "ann");
    assert_eq!(rows[0].get::<_, i64>("n"), 2);
    assert_eq!(rows[2].get::<_, i64>("n"), 0);
    let rows = c
        .query(
            "SELECT name FROM authors WHERE id IN (SELECT author_id FROM books) ORDER BY name",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    let row = c
        .query_one("SELECT (SELECT count(*) FROM books b WHERE b.author_id = a.id) FROM authors a WHERE a.id = 1", &[])
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
    // A foreign key is enforced.
    let e = c.execute("INSERT INTO books VALUES (9, 99, 'x', 1)", &[]).unwrap_err();
    assert_eq!(e.as_db_error().unwrap().code().code(), "23503");
}

#[test]
fn portal_suspension_with_row_limit() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int); INSERT INTO t SELECT * FROM generate_series(1, 25)")
        .unwrap();
    // portal_iter drives Execute with a row limit, suspending in between.
    let mut tx = c.transaction().unwrap();
    let portal = tx.bind("SELECT id FROM t ORDER BY id", &[]).unwrap();
    let first = tx.query_portal(&portal, 10).unwrap();
    assert_eq!(first.len(), 10);
    let rest = tx.query_portal(&portal, 100).unwrap();
    assert_eq!(rest.len(), 15);
    assert_eq!(first[0].get::<_, i32>(0), 1);
    assert_eq!(rest[14].get::<_, i32>(0), 25);
}

#[test]
fn catalog_queries_drivers_send() {
    let mut c = client();
    c.batch_execute(
        "CREATE TABLE t (id serial primary key, name varchar(20) not null, data jsonb)",
    )
    .unwrap();
    let rows = c
        .query(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns
             WHERE table_name = 't' ORDER BY ordinal_position",
            &[],
        )
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, vec!["id".to_string(), "name".to_string(), "data".to_string()]);
    assert_eq!(rows[1].get::<_, &str>(1), "character varying");
    assert_eq!(rows[1].get::<_, &str>(2), "NO");

    let row =
        c.query_one("SELECT oid, relname, relkind FROM pg_class WHERE relname = 't'", &[]).unwrap();
    assert!(row.get::<_, u32>(0) > 0);
    assert_eq!(row.get::<_, i8>(2), b'r' as i8);

    let rows = c
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
             WHERE a.attrelid = 't'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            &[],
        )
        .unwrap();
    assert_eq!(rows[1].get::<_, &str>(1), "character varying(20)");

    // The primary key, as ORMs look it up.
    let rows = c
        .query(
            "SELECT c.conname, c.contype FROM pg_constraint c WHERE c.conrelid = 't'::regclass AND c.contype = 'p'",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "t_pkey");
}

#[test]
fn settings_and_show() {
    let mut c = client();
    let row = c.query_one("SHOW server_version", &[]).unwrap();
    assert!(row.get::<_, &str>(0).starts_with("16"));
    c.batch_execute("SET TimeZone = 'UTC'").unwrap();
    assert_eq!(c.query_one("SHOW TimeZone", &[]).unwrap().get::<_, &str>(0), "UTC");
    c.batch_execute("SET search_path = public, pg_catalog").unwrap();
    assert_eq!(c.query_one("SELECT current_schema()", &[]).unwrap().get::<_, &str>(0), "public");
    let row = c.query_one("SELECT current_setting('server_version_num')", &[]).unwrap();
    assert!(row.get::<_, &str>(0).starts_with("16"));
}

#[test]
fn null_parameters_and_types() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int, name text)").unwrap();
    let none: Option<&str> = None;
    c.execute("INSERT INTO t VALUES ($1, $2)", &[&1i32, &none]).unwrap();
    let row = c.query_one("SELECT name FROM t WHERE id = $1", &[&1i32]).unwrap();
    assert_eq!(row.get::<_, Option<String>>(0), None);
    // A parameter used only in a projection defaults to text.
    let row = c.query_one("SELECT $1::text AS v", &[&"x"]).unwrap();
    assert_eq!(row.get::<_, &str>(0), "x");
}

#[test]
fn prepared_statements_are_reusable() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int); INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let stmt =
        c.prepare_typed("SELECT id FROM t WHERE id > $1 ORDER BY id", &[Type::INT4]).unwrap();
    for (arg, expected) in [(0i32, 3usize), (1, 2), (2, 1), (3, 0)] {
        let params: Vec<&(dyn ToSql + Sync)> = vec![&arg];
        assert_eq!(c.query(&stmt, &params).unwrap().len(), expected);
    }
}

/// The engine re-binds every statement at execute time (see
/// `engine::run_one`), and that re-bind must reuse the parameter types
/// `Prepared::param_types` already resolved rather than re-deriving types
/// from the decoded runtime values. Found via a real Gitea (xorm) INSERT:
/// an unspecified-type parameter (the driver leaves it for the server to
/// resolve, exactly what `c.execute` below does for `$2`) bound to a
/// `json` column decodes as a `Value::Text` (json's own runtime
/// representation), which a value-shape guess reports as plain `text` —
/// and `text -> json` isn't even a valid assignment cast, so the very
/// column the value was headed for rejected it on re-bind.
#[test]
fn unspecified_param_type_resolves_consistently_on_rebind() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int, meta json)").unwrap();
    let empty_array = serde_json::json!([]);
    c.execute("INSERT INTO t (id, meta) VALUES ($1, $2)", &[&1i32, &empty_array]).unwrap();
    let row = c.query_one("SELECT meta FROM t WHERE id = $1", &[&1i32]).unwrap();
    assert_eq!(row.get::<_, serde_json::Value>(0), serde_json::json!([]));
    // Re-executing the same prepared shape (a second `execute` of the same
    // SQL text reuses the unnamed statement) must resolve identically, not
    // just on the first bind.
    let empty_obj = serde_json::json!({});
    c.execute("INSERT INTO t (id, meta) VALUES ($1, $2)", &[&2i32, &empty_obj]).unwrap();
    let row = c.query_one("SELECT meta FROM t WHERE id = $1", &[&2i32]).unwrap();
    assert_eq!(row.get::<_, serde_json::Value>(0), serde_json::json!({}));
}

/// A second bug surfaced fixing the first: even a parameter whose type
/// the *client* declares explicitly (not left unspecified) must still get
/// its target column's own `typmod` (e.g. a `numeric(p,s)` column's
/// scale) applied on every re-bind, not just resolved once and forgotten.
#[test]
fn concretely_typed_param_gets_column_typmod_on_every_rebind() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id int, price numeric(8,2))").unwrap();
    // Declare $1 as int4 explicitly — a type other than the target
    // column's own numeric(8,2) — so the server must coerce and scale it
    // at the point of use each time, the same way `prepare` itself did.
    let ins = c
        .prepare_typed("INSERT INTO t (id, price) VALUES ($1, $2)", &[Type::INT4, Type::INT4])
        .unwrap();
    c.execute(&ins, &[&1i32, &5i32]).unwrap();
    let row = c.query_one("SELECT price::text FROM t WHERE id = $1", &[&1i32]).unwrap();
    assert_eq!(row.get::<_, String>(0), "5.00");
    c.execute(&ins, &[&2i32, &11i32]).unwrap();
    let row = c.query_one("SELECT price::text FROM t WHERE id = $1", &[&2i32]).unwrap();
    assert_eq!(row.get::<_, String>(0), "11.00");
    let upd = c
        .prepare_typed("UPDATE t SET price = $1 WHERE id = $2", &[Type::INT4, Type::INT4])
        .unwrap();
    c.execute(&upd, &[&7i32, &1i32]).unwrap();
    let row = c.query_one("SELECT price::text FROM t WHERE id = $1", &[&1i32]).unwrap();
    assert_eq!(row.get::<_, String>(0), "7.00");
}

/// `INSERT INTO t (...) SELECT $1, $2, ...` (as opposed to `VALUES`): an
/// unspecified-type parameter directly in the target list must resolve
/// against that column's own type, the same way one in a `VALUES` row
/// already does. Found via Miniflux's own entry-insert query, which uses
/// exactly this shape (`INSERT ... SELECT $1, ... WHERE NOT EXISTS (...)`,
/// an atomicity idiom) with a `time.Time` parameter headed for a
/// `timestamptz` column.
#[test]
fn unspecified_param_in_insert_select_resolves_against_target_column() {
    let mut c = client();
    c.batch_execute("CREATE TABLE t (id serial primary key, ts timestamptz, body text)").unwrap();
    let now = std::time::SystemTime::now();
    c.execute(
        "INSERT INTO t (ts, body) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM t WHERE body = $2)",
        &[&now, &"hello"],
    )
    .unwrap();
    let row = c.query_one("SELECT body FROM t WHERE id = 1", &[]).unwrap();
    assert_eq!(row.get::<_, String>(0), "hello");
}

#[test]
fn two_connections_share_data() {
    let addr = start();
    let mut a = connect(addr);
    let mut b = connect(addr);
    a.batch_execute("CREATE TABLE t (id int)").unwrap();
    a.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
    assert_eq!(b.query_one("SELECT count(*) FROM t", &[]).unwrap().get::<_, i64>(0), 1);
    // Uncommitted work is invisible to the other session.
    let mut tx = a.transaction().unwrap();
    tx.execute("INSERT INTO t VALUES (2)", &[]).unwrap();
    assert_eq!(b.query_one("SELECT count(*) FROM t", &[]).unwrap().get::<_, i64>(0), 1);
    tx.commit().unwrap();
    assert_eq!(b.query_one("SELECT count(*) FROM t", &[]).unwrap().get::<_, i64>(0), 2);
}
