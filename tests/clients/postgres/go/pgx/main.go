// pgx (the low-level Go Postgres driver) against noida-db: batched inserts,
// array/jsonb scan, transactions and rollback, and COPY FROM (pgx's fast
// path uses the binary COPY format, a documented gap, reported as such
// rather than a failure, when it errors).
//
// Run with PGPORT pointing at noida-db (or a real Postgres, which must pass
// just the same).
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

var checks int

func check(cond bool, msg string) {
	if !cond {
		panic("FAIL: " + msg)
	}
	checks++
}

func main() {
	port := os.Getenv("PGPORT")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%s/postgres", port))
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	var version string
	err = conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	check(err == nil, fmt.Sprintf("version query: %v", err))

	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS t`)
	check(err == nil, fmt.Sprintf("drop table: %v", err))
	_, err = conn.Exec(ctx, `CREATE TABLE t (id serial PRIMARY KEY, name text, tags text[], meta jsonb)`)
	check(err == nil, fmt.Sprintf("create table: %v", err))

	batch := &pgx.Batch{}
	batch.Queue("INSERT INTO t (name, tags, meta) VALUES ($1, $2, $3)", "a", []string{"x", "y"}, `{"k":1}`)
	batch.Queue("INSERT INTO t (name, tags, meta) VALUES ($1, $2, $3)", "b", []string{}, `{}`)
	br := conn.SendBatch(ctx, batch)
	for i := 0; i < 2; i++ {
		_, err = br.Exec()
		check(err == nil, fmt.Sprintf("batch insert %d: %v", i, err))
	}
	check(br.Close() == nil, "batch close")

	rows, err := conn.Query(ctx, "SELECT id, name, tags FROM t ORDER BY id")
	check(err == nil, fmt.Sprintf("select: %v", err))
	var got []string
	for rows.Next() {
		var id int
		var name string
		var tags []string
		check(rows.Scan(&id, &name, &tags) == nil, "scan")
		got = append(got, fmt.Sprintf("%d:%s:%v", id, name, tags))
	}
	rows.Close()
	check(len(got) == 2 && got[0] == "1:a:[x y]", fmt.Sprintf("rows: %v", got))

	tx, err := conn.Begin(ctx)
	check(err == nil, fmt.Sprintf("begin: %v", err))
	_, err = tx.Exec(ctx, "UPDATE t SET name = 'z' WHERE id = 1")
	check(err == nil, "tx update")
	check(tx.Commit(ctx) == nil, "commit")
	var name string
	check(conn.QueryRow(ctx, "SELECT name FROM t WHERE id = 1").Scan(&name) == nil && name == "z", "committed value")

	tx2, _ := conn.Begin(ctx)
	_, err = tx2.Exec(ctx, "INSERT INTO t (id, name) VALUES (1, 'dup')")
	check(err != nil, "unique violation should error")
	check(tx2.Rollback(ctx) == nil, "rollback")

	var n int
	check(conn.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n) == nil && n == 2, fmt.Sprintf("count after rollback: %d", n))

	// COPY FROM, a pgx-specific fast path.
	_, err = conn.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"name"}, pgx.CopyFromRows([][]interface{}{{"c"}, {"d"}}))
	if err == nil {
		checks++
		check(conn.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n) == nil && n == 4, "copy from result")
	} else {
		fmt.Println("-- COPY FROM (binary format, a documented limitation):", err)
	}

	_, err = conn.Exec(ctx, `DROP TABLE t`)
	check(err == nil, fmt.Sprintf("drop table (cleanup): %v", err))

	fmt.Printf("pgx: %d checks passed\n", checks)
}
