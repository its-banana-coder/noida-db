//! The official Rust `clickhouse` crate against noida-db's HTTP interface.
//! It requires `RowBinaryWithNamesAndTypes` for `fetch`/`fetch_all` (added
//! in milestone 3 — see src/clickhouse/rowbinary.rs). Validated inserts
//! (`.with_validation(true)`, the crate's default) first run `DESCRIBE
//! TABLE` to check the target schema — see src/clickhouse/engine.rs's
//! `do_describe_table` — so this no longer needs to disable validation.

mod common;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

fn client() -> clickhouse::Client {
    let addr = common::start_noida_clickhouse();
    // The crate defaults to lz4-compressed responses (ClickHouse's own
    // block compression, not HTTP content-encoding); that's not built here
    // yet, so ask for plain bodies.
    clickhouse::Client::default()
        .with_url(format!("http://{addr}"))
        .with_compression(clickhouse::Compression::None)
}

#[derive(Row, Deserialize)]
struct NumberRow {
    number: u64,
}

#[tokio::test]
async fn fetch_all_from_numbers() {
    let rows: Vec<NumberRow> =
        client().query("SELECT number FROM numbers(5)").fetch_all().await.unwrap();
    let nums: Vec<u64> = rows.iter().map(|r| r.number).collect();
    assert_eq!(nums, [0, 1, 2, 3, 4]);
}

#[derive(Row, Deserialize)]
struct OneRow {
    dummy: u8,
}

#[tokio::test]
async fn fetch_one_from_system_one() {
    let row: OneRow = client().query("SELECT * FROM system.one").fetch_one().await.unwrap();
    assert_eq!(row.dummy, 0);
}

#[derive(Row, Serialize, Deserialize)]
struct Event {
    id: u32,
    kind: String,
}

#[tokio::test]
async fn insert_then_select() {
    let client = client();
    client
        .query("CREATE TABLE events (id UInt32, kind String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    // Validation is on by default: the client issues a DESCRIBE TABLE
    // first to check the target schema before inserting.
    let mut insert = client.insert::<Event>("events").await.unwrap();
    insert.write(&Event { id: 1, kind: "click".into() }).await.unwrap();
    insert.write(&Event { id: 2, kind: "view".into() }).await.unwrap();
    insert.end().await.unwrap();

    #[derive(Row, Deserialize)]
    struct CountRow {
        n: u64,
    }
    let row: CountRow = client.query("SELECT count(*) AS n FROM events").fetch_one().await.unwrap();
    assert_eq!(row.n, 2);
}

/// A second validated insert into the same table, from a struct field order
/// that doesn't match the table's declared column order — this only works
/// because validation maps fields by name using the `DESCRIBE TABLE`
/// response, not by position.
#[tokio::test]
async fn validated_insert_with_reordered_struct_fields() {
    let client = client();
    client
        .query("CREATE TABLE reordered (id UInt32, kind String) ENGINE = Memory")
        .execute()
        .await
        .unwrap();

    #[derive(Row, Serialize, Deserialize)]
    struct ReorderedEvent {
        kind: String,
        id: u32,
    }

    let mut insert = client.insert::<ReorderedEvent>("reordered").await.unwrap();
    insert.write(&ReorderedEvent { kind: "click".into(), id: 1 }).await.unwrap();
    insert.end().await.unwrap();

    #[derive(Row, Deserialize)]
    struct Got {
        id: u32,
        kind: String,
    }
    let row: Got = client.query("SELECT id, kind FROM reordered").fetch_one().await.unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.kind, "click");
}
