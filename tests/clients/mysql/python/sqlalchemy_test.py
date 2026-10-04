"""SQLAlchemy (pymysql driver) against noida-db's MySQL: create_all, ORM
round trip with relationships, Enum/JSON/Numeric types, reflection, Core
executemany/IN-expanding, transaction rollback and IntegrityError."""
import datetime
import decimal

import sqlalchemy as sa
from harness import PORT, case, report
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

eng = sa.create_engine(f"mysql+pymysql://root@127.0.0.1:{PORT}/test")


class Base(DeclarativeBase):
    pass


class Author(Base):
    __tablename__ = "authors"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(50), unique=True)
    created: Mapped[datetime.datetime] = mapped_column(server_default=sa.func.now())
    books: Mapped[list["Book"]] = relationship(back_populates="author", cascade="all, delete-orphan")


class Book(Base):
    __tablename__ = "books"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(sa.String(100))
    price: Mapped[decimal.Decimal] = mapped_column(sa.Numeric(8, 2))
    status: Mapped[str] = mapped_column(sa.Enum("draft", "live", name="st"), default="draft")
    meta: Mapped[dict] = mapped_column(sa.JSON, nullable=True)
    author_id: Mapped[int] = mapped_column(sa.ForeignKey("authors.id"))
    author: Mapped[Author] = relationship(back_populates="books")


case("connect", lambda: eng.connect().close())
case("create_all", lambda: Base.metadata.create_all(eng))
case("create_all again (checkfirst)", lambda: Base.metadata.create_all(eng))


def orm():
    with Session(eng) as s:
        a = Author(name="ann", books=[Book(title="x", price=decimal.Decimal("9.99"), meta={"tags": ["a"]}),
                                      Book(title="y", price=decimal.Decimal("5.01"), status="live")])
        s.add(a)
        s.commit()
        total = s.scalar(sa.select(sa.func.sum(Book.price)).join(Author).where(Author.name == "ann"))
        live = s.scalars(sa.select(Book.title).where(Book.status == "live")).all()
        meta = s.scalar(sa.select(Book.meta).where(Book.title == "x"))
        aid = a.id
        a.name = "anne"
        s.commit()
        s.expire_all()
        reread = s.get(Author, aid)
        n_books = len(reread.books)
        s.delete(reread)
        s.commit()
        left = s.scalar(sa.select(sa.func.count()).select_from(Book))
    return total, live, meta, reread.name, n_books, left


case("ORM round trip (join, enum, json, update, cascade delete)", orm,
     (decimal.Decimal("15.00"), ["y"], {"tags": ["a"]}, "anne", 2, 0))


def reflect():
    insp = sa.inspect(eng)
    tables = sorted(t for t in insp.get_table_names() if t in ("authors", "books"))
    cols = [(c["name"], str(c["type"])) for c in insp.get_columns("books")]
    pk = insp.get_pk_constraint("books")["constrained_columns"]
    uniq = insp.get_unique_constraints("authors")
    return tables, cols[:3], pk, [u["column_names"] for u in uniq]


case("inspector reflection", reflect, (["authors", "books"], [("id", "INTEGER"), ("title", "VARCHAR(100)"), ("price", "DECIMAL(8, 2)")],
                                      ["id"], [["name"]]))


def core():
    with eng.begin() as c:
        c.execute(sa.text("INSERT INTO authors (name) VALUES (:n)"), [{"n": f"a{i}"} for i in range(5)])
        r = c.execute(sa.text("SELECT name FROM authors WHERE name LIKE :p ORDER BY name LIMIT 2 OFFSET 1"), {"p": "a%"}).all()
        up = c.execute(sa.text("UPDATE authors SET name = CONCAT(name, '!') WHERE name IN :names").bindparams(
            sa.bindparam("names", expanding=True)), {"names": ["a1", "a2"]}).rowcount
    return [x[0] for x in r], up


case("Core executemany, LIKE, IN expanding, rowcount", core, (["a1", "a2"], 2))


def tx_rollback():
    try:
        with eng.begin() as c:
            c.execute(sa.text("INSERT INTO authors (name) VALUES ('rollme')"))
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    with eng.connect() as c:
        return c.execute(sa.text("SELECT COUNT(*) FROM authors WHERE name = 'rollme'")).scalar()


case("transaction rollback", tx_rollback, 0)


def integrity_error():
    try:
        with eng.begin() as c:
            c.execute(sa.text("INSERT INTO authors (name) VALUES ('a0')"))
    except sa.exc.IntegrityError as e:
        return e.orig.args[0]
    return None


case("IntegrityError on dup unique", integrity_error, 1062)
case("drop_all", lambda: Base.metadata.drop_all(eng))
report("sqlalchemy")
