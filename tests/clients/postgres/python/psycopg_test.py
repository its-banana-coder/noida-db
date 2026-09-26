"""psycopg 3 against noida: parameterized CRUD, transactions, types, catalogs.

Run with PGPORT pointing at noida (or at a real Postgres, which must pass
just the same).
"""

import datetime
import decimal
import os
import sys
import uuid

import psycopg
from psycopg.rows import dict_row

PORT = int(os.environ.get("PGPORT", "5432"))
DSN = f"host=127.0.0.1 port={PORT} user=postgres dbname=postgres password=postgres"

checks = 0


def check(what, got, want):
    global checks
    checks += 1
    if got != want:
        raise AssertionError(f"{what}: got {got!r}, want {want!r}")


def main():
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS items")
        conn.execute("DROP TABLE IF EXISTS people")
        # DDL and the server_version the driver negotiated.
        check("server_version", conn.info.server_version // 10000, 16)
        conn.execute(
            """
            CREATE TABLE people (
                id serial PRIMARY KEY,
                name text NOT NULL,
                email varchar(64) UNIQUE,
                age int,
                score numeric(6,2),
                active bool DEFAULT true,
                tags text[],
                data jsonb,
                uid uuid,
                born date,
                seen timestamptz
            )
            """
        )

        # Parameterized INSERT through the extended query protocol.
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO people (name, email, age, score, tags, data, uid, born, seen)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (
                    "ann",
                    "ann@example.com",
                    31,
                    decimal.Decimal("12.50"),
                    ["a", "b"],
                    psycopg.types.json.Jsonb({"k": [1, 2], "z": None}),
                    uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
                    datetime.date(1993, 2, 28),
                    datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
                ),
            )
            check("returning id", cur.fetchone()[0], 1)

        cur = conn.execute(
            "INSERT INTO people (name, email, age) VALUES (%s, %s, %s) RETURNING id, active",
            ("bob", "bob@example.com", 25),
        )
        check("defaults", cur.fetchone(), (2, True))

        # Types survive the round trip.
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM people WHERE name = %s", ("ann",))
            row = cur.fetchone()
            check("text", row["name"], "ann")
            check("varchar", row["email"], "ann@example.com")
            check("int", row["age"], 31)
            check("numeric", row["score"], decimal.Decimal("12.50"))
            check("bool", row["active"], True)
            check("array", row["tags"], ["a", "b"])
            check("jsonb", row["data"], {"k": [1, 2], "z": None})
            check("uuid", str(row["uid"]), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
            check("date", row["born"], datetime.date(1993, 2, 28))
            check(
                "timestamptz",
                row["seen"],
                datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
            )

        # UPDATE / DELETE with parameters and rowcount.
        cur = conn.execute("UPDATE people SET age = age + %s WHERE name = %s", (1, "bob"))
        check("update rowcount", cur.rowcount, 1)
        check("updated", conn.execute("SELECT age FROM people WHERE name = 'bob'").fetchone()[0], 26)
        cur = conn.execute("DELETE FROM people WHERE age > %s", (100,))
        check("delete rowcount", cur.rowcount, 0)

        # executemany and a server-side prepared statement.
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO people (name, age) VALUES (%s, %s)",
                [("cid", 40), ("dee", 50), ("eve", 60)],
            )
            cur.execute("SELECT count(*) FROM people WHERE age >= %s", (40,), prepare=True)
            check("prepared count", cur.fetchone()[0], 3)
            cur.execute("SELECT count(*) FROM people WHERE age >= %s", (50,), prepare=True)
            check("prepared count again", cur.fetchone()[0], 2)

        # Aggregates, grouping and ordering.
        rows = conn.execute(
            "SELECT active, count(*), max(age) FROM people GROUP BY active ORDER BY active"
        ).fetchall()
        check("group by", rows, [(True, 5, 60)])

        # Errors carry SQLSTATE, and the connection stays usable.
        try:
            conn.execute("INSERT INTO people (name, email) VALUES ('x', 'ann@example.com')")
            raise AssertionError("expected a unique violation")
        except psycopg.errors.UniqueViolation as e:
            check("unique sqlstate", e.sqlstate, "23505")
        check("after error", conn.execute("SELECT 1").fetchone()[0], 1)

        try:
            conn.execute("SELECT * FROM missing_table")
            raise AssertionError("expected an undefined table error")
        except psycopg.errors.UndefinedTable as e:
            check("undefined table sqlstate", e.sqlstate, "42P01")

    # Transactions: commit, rollback and savepoints.
    with psycopg.connect(DSN) as conn:
        conn.execute("CREATE TABLE items (id int primary key, n int)")
        conn.execute("INSERT INTO items VALUES (1, 1)")
        conn.commit()
        conn.execute("INSERT INTO items VALUES (2, 2)")
        conn.rollback()
        check("rollback", conn.execute("SELECT count(*) FROM items").fetchone()[0], 1)
        with conn.transaction():
            conn.execute("INSERT INTO items VALUES (3, 3)")
            try:
                with conn.transaction():
                    conn.execute("INSERT INTO items VALUES (4, 4)")
                    raise RuntimeError("rollback the savepoint")
            except RuntimeError:
                pass
        check("savepoint", sorted(r[0] for r in conn.execute("SELECT id FROM items").fetchall()), [1, 3])
        conn.commit()

        # COPY-free bulk insert with a multi-row VALUES list.
        conn.execute("INSERT INTO items (id, n) SELECT g, g * 2 FROM generate_series(10, 14) g")
        conn.commit()
        check("bulk", conn.execute("SELECT count(*) FROM items WHERE id >= 10").fetchone()[0], 5)

        # ON CONFLICT upsert.
        conn.execute(
            "INSERT INTO items (id, n) VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET n = excluded.n"
        )
        conn.commit()
        check("upsert", conn.execute("SELECT n FROM items WHERE id = 1").fetchone()[0], 100)

    # Catalog introspection, the way tools read a schema.
    with psycopg.connect(DSN, autocommit=True) as conn:
        cols = conn.execute(
            """SELECT column_name, data_type, is_nullable, character_maximum_length
               FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = 'people'
               ORDER BY ordinal_position"""
        ).fetchall()
        check("column count", len(cols), 11)
        check("id column", cols[0][:3], ("id", "integer", "NO"))
        check("email column", cols[2], ("email", "character varying", "YES", 64))

        pk = conn.execute(
            """SELECT a.attname
               FROM pg_index i
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
               WHERE i.indrelid = 'people'::regclass AND i.indisprimary"""
        ).fetchall()
        check("primary key", pk, [("id",)])

        tables = conn.execute(
            """SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"""
        ).fetchall()
        check("pg_tables", tables, [("items",), ("people",)])

        # psycopg's own type introspection on connect.
        oid = conn.execute("SELECT 'jsonb'::regtype::oid").fetchone()[0]
        check("jsonb oid", oid, 3802)

        conn.execute("DROP TABLE items")
        conn.execute("DROP TABLE people")

    print(f"psycopg: {checks} checks passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"psycopg FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        raise
