//! The official Rust `clickhouse` crate against noida-db's HTTP interface.
//! It requires `RowBinaryWithNamesAndTypes` for `fetch`/`fetch_all` (added
//! in milestone 3 — see src/clickhouse/rowbinary.rs) and plain `RowBinary`
//! for `insert` with validation off (validated inserts first run
//! `DESCRIBE TABLE`, which isn't built yet).

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

    // Validated inserts issue a DESCRIBE TABLE first, which isn't built
    // yet; validation off falls back to plain RowBinary (field order must
    // match the table's column order, which it does here).
    let unvalidated = client.clone().with_validation(false);
    let mut insert = unvalidated.insert::<Event>("events").await.unwrap();
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
