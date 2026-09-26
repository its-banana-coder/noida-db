"""SQLAlchemy (Core, ORM and reflection) on psycopg 3 against noida."""

import os
import sys

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship

PORT = int(os.environ.get("PGPORT", "5432"))
URL = f"postgresql+psycopg://postgres:postgres@127.0.0.1:{PORT}/postgres"

checks = 0


def check(what, got, want):
    global checks
    checks += 1
    if got != want:
        raise AssertionError(f"{what}: got {got!r}, want {want!r}")


class Base(DeclarativeBase):
    pass


class Author(Base):
    __tablename__ = "sa_authors"
    id = Column(Integer, primary_key=True)
    name = Column(String(40), nullable=False, unique=True)
    books = relationship("Book", back_populates="author", cascade="all, delete-orphan")


class Book(Base):
    __tablename__ = "sa_books"
    id = Column(Integer, primary_key=True)
    title = Column(String(80), nullable=False)
    price = Column(Numeric(8, 2))
    in_print = Column(Boolean, default=True)
    published = Column(DateTime(timezone=True))
    author_id = Column(Integer, ForeignKey("sa_authors.id"))
    author = relationship("Author", back_populates="books")


def main():
    engine = create_engine(URL, future=True)

    # Connecting alone makes SQLAlchemy read the server version and settings.
    with engine.connect() as conn:
        check("select 1", conn.execute(text("SELECT 1")).scalar_one(), 1)
        check("dialect", engine.dialect.name, "postgresql")
        check("server version", engine.dialect.server_version_info[0], 16)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # ORM writes and reads.
    with Session(engine) as session:
        ann = Author(name="ann")
        ann.books = [
            Book(title="first", price=10.50),
            Book(title="second", price=20),
        ]
        session.add(ann)
        session.add(Author(name="bob"))
        session.commit()

        authors = session.scalars(select(Author).order_by(Author.name)).all()
        check("authors", [a.name for a in authors], ["ann", "bob"])
        check("lazy load books", sorted(b.title for b in authors[0].books), ["first", "second"])

        # A join with an aggregate, the shape ORMs emit for dashboards.
        rows = session.execute(
            select(Author.name, func.count(Book.id), func.coalesce(func.sum(Book.price), 0))
            .join(Book, Book.author_id == Author.id, isouter=True)
            .group_by(Author.name)
            .order_by(Author.name)
        ).all()
        check("grouped", [(r[0], r[1]) for r in rows], [("ann", 2), ("bob", 0)])
        check("sum", str(rows[0][2]), "30.50")

        # Update and delete through the unit of work.
        book = session.scalars(select(Book).where(Book.title == "first")).one()
        book.price = 11
        session.commit()
        check("updated", str(session.scalars(select(Book.price).where(Book.title == "first")).one()), "11.00")

        session.delete(authors[1])
        session.commit()
        check("deleted", session.scalar(select(func.count()).select_from(Author)), 1)

        # Rollback.
        session.add(Author(name="cid"))
        session.flush()
        session.rollback()
        check("rolled back", session.scalar(select(func.count()).select_from(Author)), 1)

    # Core with bound parameters and a transaction.
    meta = MetaData()
    tally = Table("sa_tally", meta, Column("k", String(10), primary_key=True), Column("n", Integer))
    meta.create_all(engine)
    with engine.begin() as conn:
        conn.execute(tally.insert(), [{"k": "a", "n": 1}, {"k": "b", "n": 2}])
    with engine.connect() as conn:
        total = conn.execute(select(func.sum(tally.c.n))).scalar_one()
        check("core sum", total, 3)
        one = conn.execute(select(tally.c.n).where(tally.c.k == "b")).scalar_one()
        check("core param", one, 2)

    # Reflection: SQLAlchemy's inspector reads the catalogs.
    insp = inspect(engine)
    names = sorted(n for n in insp.get_table_names() if n.startswith("sa_"))
    check("table names", names, ["sa_authors", "sa_books", "sa_tally"])
    cols = {c["name"]: c for c in insp.get_columns("sa_books")}
    check("reflected columns", sorted(cols), ["author_id", "id", "in_print", "price", "published", "title"])
    check("reflected type", str(cols["title"]["type"]), "VARCHAR(80)")
    check("reflected nullable", cols["title"]["nullable"], False)
    pk = insp.get_pk_constraint("sa_books")
    check("reflected pk", pk["constrained_columns"], ["id"])
    fks = insp.get_foreign_keys("sa_books")
    check("reflected fk", (fks[0]["constrained_columns"], fks[0]["referred_table"]), (["author_id"], "sa_authors"))
    uniques = insp.get_unique_constraints("sa_authors")
    check("reflected unique", uniques[0]["column_names"], ["name"])

    # Reflecting into a fresh MetaData is what Alembic does on autogenerate.
    reflected = MetaData()
    reflected.reflect(bind=engine, only=lambda name, _m: name.startswith("sa_"))
    check("reflected tables", sorted(reflected.tables), ["sa_authors", "sa_books", "sa_tally"])

    meta.drop_all(engine)
    Base.metadata.drop_all(engine)
    engine.dispose()
    print(f"sqlalchemy: {checks} checks passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"sqlalchemy FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        raise
