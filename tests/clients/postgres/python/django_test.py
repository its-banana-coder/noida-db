"""Django 5 against noida-db: real migrations (built-in apps included), schema
changes, and a wide slice of the ORM.

Run with PGPORT set; run.sh does that. The project lives in a temp directory.
"""

import os
import sys
import tempfile
import textwrap

port = int(os.environ["PGPORT"])
project = tempfile.mkdtemp(prefix="noida_django_")
sys.path.insert(0, project)

os.makedirs(os.path.join(project, "shop", "migrations"))
for name in ("shop/__init__.py", "shop/migrations/__init__.py"):
    open(os.path.join(project, name), "w").close()

MODELS_V1 = textwrap.dedent(
    """
    from django.db import models

    class Author(models.Model):
        name = models.CharField(max_length=60, unique=True)
        born = models.DateField(null=True)

    class Book(models.Model):
        author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name="books")
        title = models.CharField(max_length=120)
        price = models.DecimalField(max_digits=8, decimal_places=2)
        tags = models.JSONField(default=list)
        in_print = models.BooleanField(default=True)
        added = models.DateTimeField(auto_now_add=True)

        class Meta:
            indexes = [models.Index(fields=["title"], name="book_title_idx")]
            constraints = [
                models.CheckConstraint(condition=models.Q(price__gte=0), name="book_price_pos"),
                models.UniqueConstraint(fields=["author", "title"], name="book_unique_title"),
            ]

    class Tag(models.Model):
        label = models.SlugField(unique=True)
        books = models.ManyToManyField(Book, related_name="tag_set")
    """
)
open(os.path.join(project, "shop", "models.py"), "w").write(MODELS_V1)

import django
from django.conf import settings

settings.configure(
    DEBUG=False,
    SECRET_KEY="x",
    USE_TZ=True,
    TIME_ZONE="UTC",
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    INSTALLED_APPS=[
        "django.contrib.auth",
        "django.contrib.contenttypes",
        "django.contrib.sessions",
        "django.contrib.admin",
        "django.contrib.messages",
        "django.contrib.postgres",
        "shop",
    ],
    MIDDLEWARE=[],
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "postgres",
            "USER": "postgres",
            "PASSWORD": "x",
            "HOST": "127.0.0.1",
            "PORT": port,
        }
    },
)
django.setup()

from django.core.management import call_command

checks = 0


def check(cond, what):
    global checks
    if not cond:
        raise AssertionError(what)
    checks += 1


def main():
    from django.db import connection, transaction, IntegrityError
    from django.db.models import Avg, Count, DecimalField, F, Max, Q, Sum, Value
    from django.db.models.functions import Lower, Coalesce
    from django.contrib.postgres.search import SearchQuery, SearchRank, SearchVector

    call_command("makemigrations", "shop", verbosity=0)
    call_command("migrate", verbosity=0)
    call_command("migrate", verbosity=0)  # a second run must be a no-op

    tables = connection.introspection.table_names()
    for t in ("auth_user", "django_migrations", "shop_book", "shop_tag_books", "django_admin_log"):
        check(t in tables, f"table {t} missing: {sorted(tables)}")

    from django.contrib.auth.models import User
    from shop.models import Author, Book, Tag

    u = User.objects.create_superuser("root", "r@example.com", "pw")
    check(User.objects.get(username="root").check_password("pw"), "password")
    check(u.is_superuser, "superuser")

    a1 = Author.objects.create(name="Ann")
    a2 = Author.objects.create(name="Bob", born="1980-02-03")
    Book.objects.bulk_create(
        [
            Book(author=a1, title="Alpha", price="9.99", tags=["x", "y"]),
            Book(author=a1, title="Beta", price="19.50"),
            Book(author=a2, title="Gamma", price="5", in_print=False),
        ]
    )
    check(Book.objects.count() == 3, "count")
    check(Author.objects.get(name="Bob").born.year == 1980, "date roundtrip")
    b = Book.objects.get(title="Alpha")
    check(b.tags == ["x", "y"] and str(b.price) == "9.99", "json/decimal")
    check(b.added.tzinfo is not None, "aware datetime")

    check(list(Book.objects.filter(price__lt=10).order_by("title").values_list("title", flat=True)) == ["Alpha", "Gamma"], "filter")
    matched = Book.objects.annotate(search=SearchVector("title")).filter(search=SearchQuery("alpha"))
    check(list(matched.values_list("title", flat=True)) == ["Alpha"], f"full-text search {list(matched)}")
    ranked = dict(
        Book.objects.annotate(
            rank=SearchRank(SearchVector("title"), SearchQuery("alpha | beta", search_type="raw"))
        ).values_list("title", "rank")
    )
    check(ranked["Alpha"] > 0 and ranked["Beta"] > 0 and ranked["Gamma"] == 0, f"search rank {ranked}")
    check(Book.objects.filter(Q(title__startswith="A") | Q(in_print=False)).count() == 2, "Q")
    check(Book.objects.filter(tags__contains=["x"]).count() == 1, "json contains")
    check(Book.objects.filter(title__iexact="beta").exists(), "iexact")
    agg = Book.objects.aggregate(n=Count("id"), total=Sum("price"), top=Max("price"), avg=Avg("price"))
    check(agg["n"] == 3 and str(agg["total"]) == "34.49", f"aggregate {agg}")
    rows = list(Author.objects.annotate(n=Count("books")).order_by("name").values_list("name", "n"))
    check(rows == [("Ann", 2), ("Bob", 1)], f"annotate {rows}")
    check(Author.objects.filter(books__price__gt=15).distinct().get().name == "Ann", "join filter")
    check(Book.objects.select_related("author").get(title="Gamma").author.name == "Bob", "select_related")
    check(len(list(Author.objects.prefetch_related("books"))) == 2, "prefetch")
    check(Book.objects.annotate(t=Lower("title")).order_by("-t").first().t == "gamma", "Lower")
    check(Book.objects.aggregate(x=Coalesce(Sum("price"), Value(0, output_field=DecimalField())))["x"] > 0, "Coalesce")

    Book.objects.filter(title="Alpha").update(price=F("price") + F("price"))
    check(str(Book.objects.get(title="Alpha").price) == "19.98", "F update")
    obj, created = Author.objects.update_or_create(name="Cid", defaults={"born": "1999-09-09"})
    check(created, "update_or_create")
    check(Author.objects.get_or_create(name="Cid")[1] is False, "get_or_create")

    t1 = Tag.objects.create(label="new")
    t1.books.add(*Book.objects.all())
    check(t1.books.count() == 3 and b.tag_set.count() == 1, "m2m")

    try:
        with transaction.atomic():
            Author.objects.create(name="Ann")
        check(False, "unique violation expected")
    except IntegrityError:
        checks_ok("unique violation")
    try:
        with transaction.atomic():
            Book.objects.create(author=a1, title="Neg", price="-1")
        check(False, "check violation expected")
    except IntegrityError:
        checks_ok("check violation")
    with transaction.atomic():
        Author.objects.create(name="Rolled")
        sid = transaction.savepoint()
        Author.objects.create(name="Inner")
        transaction.savepoint_rollback(sid)
    check(Author.objects.filter(name="Rolled").exists() and not Author.objects.filter(name="Inner").exists(), "savepoints")

    a2.delete()  # cascades to the book
    check(Book.objects.count() == 2 and not Book.objects.filter(title="Gamma").exists(), "cascade delete")

    # A second migration: add a field, a unique index, rename a column.
    src = open(os.path.join(project, "shop", "models.py")).read()
    src = src.replace(
        "in_print = models.BooleanField(default=True)",
        "in_print = models.BooleanField(default=True)\n    isbn = models.CharField(max_length=20, null=True, unique=True)\n    pages = models.IntegerField(default=0)",
    )
    open(os.path.join(project, "shop", "models.py"), "w").write(src)
    import importlib
    import shop.models  # noqa: F401
    importlib.reload(shop.models)
    call_command("makemigrations", "shop", verbosity=0, name="more")
    call_command("migrate", verbosity=0)
    cols = {c.name for c in connection.introspection.get_table_description(connection.cursor(), "shop_book")}
    check({"isbn", "pages"} <= cols, f"new columns missing: {cols}")

    # Introspection.
    with connection.cursor() as cur:
        cons = connection.introspection.get_constraints(cur, "shop_book")
        kinds = {k: (v["unique"], v["primary_key"], v["foreign_key"] is not None, v["check"]) for k, v in cons.items()}
        check(any(v[1] for v in kinds.values()), f"primary key: {kinds}")
        check(any(v[2] for v in kinds.values()), f"foreign key: {kinds}")
        check("book_price_pos" in kinds and kinds["book_price_pos"][3], f"check: {kinds}")
        check("book_title_idx" in cons, f"index: {list(cons)}")
        seqs = connection.introspection.get_sequences(cur, "shop_book")
        check(any(s["column"] == "id" for s in seqs), f"sequences {seqs}")

    call_command("inspectdb", "shop_book", stdout=open(os.devnull, "w"))
    call_command("sqlmigrate", "shop", "0001", stdout=open(os.devnull, "w"))
    call_command("flush", interactive=False, verbosity=0)
    check(Book.objects.count() == 0 and User.objects.count() == 0, "flush")
    print(f"django: {checks} checks passed")


def checks_ok(_what):
    global checks
    checks += 1


try:
    main()
except Exception as e:  # noqa: BLE001
    import traceback

    traceback.print_exc()
    print(f"django FAILED: {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}")
    sys.exit(1)
