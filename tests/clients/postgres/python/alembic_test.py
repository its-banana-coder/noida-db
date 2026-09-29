"""Alembic against noida-db: create_table, add_column, alter_column (rename),
create_index, a foreign key with ON DELETE CASCADE, and the
upgrade/downgrade/upgrade cycle real projects run. Driven through Alembic's
`op` API directly rather than a scaffolded project, since it's the same
code Alembic's own migration runner calls.

Run with PGPORT pointing at noida-db (or a real Postgres, which must pass
just the same).
"""

import os

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

PORT = int(os.environ.get("PGPORT", "5432"))

checks = 0


def check(what, cond):
    global checks
    checks += 1
    if not cond:
        raise AssertionError(what)


def upgrade_0001(op):
    op.create_table(
        "am_authors",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(60), nullable=False, unique=True),
    )
    op.create_table(
        "am_books",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("author_id", sa.Integer, sa.ForeignKey("am_authors.id", ondelete="CASCADE")),
        sa.Column("price", sa.Numeric(8, 2), server_default="0"),
    )
    op.create_index("ix_am_books_title", "am_books", ["title"])


def downgrade_0001(op):
    op.drop_table("am_books")
    op.drop_table("am_authors")


def upgrade_0002(op):
    op.add_column("am_books", sa.Column("in_print", sa.Boolean, server_default="true"))
    op.alter_column("am_books", "title", new_column_name="book_title")


def downgrade_0002(op):
    op.alter_column("am_books", "book_title", new_column_name="title")
    op.drop_column("am_books", "in_print")


def main():
    engine = sa.create_engine(f"postgresql+psycopg://postgres@127.0.0.1:{PORT}/postgres")
    with engine.connect() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS am_books, am_authors CASCADE"))
        conn.commit()
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        upgrade_0001(op)
        upgrade_0002(op)
        conn.commit()
        cols = {
            r[0]
            for r in conn.execute(
                sa.text("SELECT column_name FROM information_schema.columns WHERE table_name = 'am_books'")
            )
        }
        check("upgrade 0001+0002 columns", {"book_title", "in_print", "author_id", "price"} <= cols)
        idx = list(conn.execute(sa.text("SELECT indexname FROM pg_indexes WHERE tablename = 'am_books'")))
        check("create_index", any(r[0] == "ix_am_books_title" for r in idx))

        author_id = conn.execute(
            sa.text("INSERT INTO am_authors (name) VALUES ('Ann') RETURNING id")
        ).scalar()
        conn.execute(
            sa.text("INSERT INTO am_books (book_title, author_id) VALUES ('Alpha', :a)"), {"a": author_id}
        )
        conn.commit()
        conn.execute(sa.text("DELETE FROM am_authors WHERE id = :a"), {"a": author_id})
        conn.commit()
        n = conn.execute(sa.text("SELECT count(*) FROM am_books")).scalar()
        check("ON DELETE CASCADE", n == 0)

        downgrade_0002(op)
        conn.commit()
        cols = {
            r[0]
            for r in conn.execute(
                sa.text("SELECT column_name FROM information_schema.columns WHERE table_name = 'am_books'")
            )
        }
        check("downgrade 0002", "title" in cols and "in_print" not in cols)

        upgrade_0002(op)
        conn.commit()
        cols = {
            r[0]
            for r in conn.execute(
                sa.text("SELECT column_name FROM information_schema.columns WHERE table_name = 'am_books'")
            )
        }
        check("re-upgrade 0002", "book_title" in cols and "in_print" in cols)

        downgrade_0002(op)
        downgrade_0001(op)
        conn.commit()
        exists = conn.execute(
            sa.text("SELECT count(*) FROM pg_class WHERE relname IN ('am_books', 'am_authors')")
        ).scalar()
        check("downgrade 0001 drops tables", exists == 0)

    print(f"alembic: {checks} checks passed")


main()
