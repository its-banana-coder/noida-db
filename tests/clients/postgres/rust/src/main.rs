// sqlx against noida-db: connection pooling, query_as/derive(FromRow),
// decimal and array binding/decoding, transactions (commit and rollback),
// and constraint violations. Run with PGPORT pointing at noida-db (or a
// real Postgres, which must pass just the same).
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

#[derive(sqlx::FromRow, Debug)]
struct Book {
    #[allow(dead_code)]
    id: i32,
    title: String,
    #[allow(dead_code)]
    price: rust_decimal::Decimal,
    tags: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".into());
    let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new().max_connections(5).connect(&url).await?;
    let mut checks = 0;

    sqlx::query("DROP TABLE IF EXISTS sx_books").execute(&pool).await?;
    sqlx::query(
        "CREATE TABLE sx_books (id serial PRIMARY KEY, title text NOT NULL, price numeric(8,2), tags text[])",
    )
    .execute(&pool)
    .await?;
    checks += 1;

    sqlx::query("INSERT INTO sx_books (title, price, tags) VALUES ($1, $2, $3)")
        .bind("Alpha")
        .bind(rust_decimal::Decimal::new(999, 2))
        .bind(vec!["x".to_string(), "y".to_string()])
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO sx_books (title, price) VALUES ($1, $2)")
        .bind("Beta")
        .bind(rust_decimal::Decimal::new(1950, 2))
        .execute(&pool)
        .await?;
    checks += 1;

    let books: Vec<Book> = sqlx::query_as("SELECT id, title, price, coalesce(tags, '{}') AS tags FROM sx_books ORDER BY id")
        .fetch_all(&pool)
        .await?;
    assert_eq!(books.len(), 2, "row count");
    assert_eq!(books[0].title, "Alpha");
    assert_eq!(books[0].tags, vec!["x".to_string(), "y".to_string()]);
    checks += 1;

    let row = sqlx::query("SELECT count(*) AS n FROM sx_books WHERE price > $1")
        .bind(rust_decimal::Decimal::new(1000, 2))
        .fetch_one(&pool)
        .await?;
    let n: i64 = row.get("n");
    assert_eq!(n, 1, "parameterized count");
    checks += 1;

    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE sx_books SET price = price + 1 WHERE title = 'Alpha'")
        .execute(&mut *tx)
        .await?;
    tx.rollback().await?;
    let row = sqlx::query("SELECT price FROM sx_books WHERE title = 'Alpha'").fetch_one(&pool).await?;
    let price: rust_decimal::Decimal = row.get("price");
    assert_eq!(price, rust_decimal::Decimal::new(999, 2), "rollback");
    checks += 1;

    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE sx_books SET price = 5.00 WHERE title = 'Beta'").execute(&mut *tx).await?;
    tx.commit().await?;
    let row = sqlx::query("SELECT price FROM sx_books WHERE title = 'Beta'").fetch_one(&pool).await?;
    let price: rust_decimal::Decimal = row.get("price");
    assert_eq!(price, rust_decimal::Decimal::new(500, 2), "commit");
    checks += 1;

    let dup = sqlx::query("INSERT INTO sx_books (id, title) VALUES (1, 'dup')").execute(&pool).await;
    assert!(dup.is_err(), "unique violation should error");
    checks += 1;

    println!("sqlx: {checks} checks passed");
    Ok(())
}
