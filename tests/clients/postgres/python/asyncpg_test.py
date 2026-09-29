"""asyncpg against noida-db: the async driver behind FastAPI/Starlette apps,
SQLAlchemy's async engine and Tortoise/edgedb-style codebases. Binary
protocol, prepared statements, pipelining via cursors, and LISTEN/NOTIFY.

Run with PGPORT pointing at noida-db (or at a real Postgres, which must pass
just the same).
"""

import asyncio
import os

import asyncpg

PORT = int(os.environ.get("PGPORT", "5432"))

checks = 0


def check(what, cond):
    global checks
    checks += 1
    if not cond:
        raise AssertionError(what)


async def main():
    conn = await asyncpg.connect(host="127.0.0.1", port=PORT, user="postgres", database="postgres")
    version = await conn.fetchval("SELECT version()")
    check("version", "PostgreSQL" in version)

    await conn.execute(
        "CREATE TABLE t (id serial PRIMARY KEY, name text, tags text[], meta jsonb, "
        "created timestamptz DEFAULT now())"
    )
    await conn.executemany(
        "INSERT INTO t (name, tags, meta) VALUES ($1, $2, $3)",
        [("a", ["x", "y"], '{"k": 1}'), ("b", [], "{}")],
    )
    rows = await conn.fetch("SELECT id, name, tags FROM t ORDER BY id")
    check("fetch rows/types", len(rows) == 2 and rows[0]["name"] == "a" and rows[0]["tags"] == ["x", "y"])
    row = await conn.fetchrow("SELECT count(*) AS n FROM t")
    check("fetchrow", row["n"] == 2)

    async with conn.transaction():
        await conn.execute("UPDATE t SET name = 'z' WHERE id = 1")
    check("transaction commit", await conn.fetchval("SELECT name FROM t WHERE id = 1") == "z")

    try:
        async with conn.transaction():
            await conn.execute("INSERT INTO t (id, name) VALUES (99, 'dup')")
            raise RuntimeError("force rollback")
    except RuntimeError:
        pass
    check("transaction rollback", await conn.fetchval("SELECT count(*) FROM t") == 2)

    stmt = await conn.prepare("SELECT $1::int + $2::int")
    check("prepared statement", await stmt.fetchval(3, 4) == 7)

    seen = []
    async with conn.transaction():
        async for rec in conn.cursor("SELECT id FROM t ORDER BY id"):
            seen.append(rec["id"])
    check("server-side cursor", seen == [1, 2])

    notified = []
    await conn.add_listener("chan1", lambda *a: notified.append(a))
    await conn.execute("NOTIFY chan1, 'hi'")
    await asyncio.sleep(0.2)
    check("LISTEN/NOTIFY", len(notified) == 1)

    await conn.close()
    print(f"asyncpg: {checks} checks passed")


asyncio.run(main())
