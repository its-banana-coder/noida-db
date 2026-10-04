"""Django 5 against noida-db's MySQL (pymysql standing in for mysqlclient):
makemigrations + migrate (contrib apps ALTER their tables), ORM queries,
atomic() and nested savepoints, IntegrityError, auth, JSONField lookups."""
import decimal
import os
import shutil
import sys

import pymysql
from harness import PORT, case, report

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(here, "django_app"))
shutil.rmtree(os.path.join(here, "django_app", "shop", "migrations"), ignore_errors=True)
pymysql.connect(host="127.0.0.1", port=PORT, user="root", autocommit=True).cursor().execute("CREATE DATABASE IF NOT EXISTS djdb")
os.environ["DJANGO_SETTINGS_MODULE"] = "settings"

import django  # noqa: E402

django.setup()
from django.contrib.auth.models import User  # noqa: E402
from django.core.management import call_command  # noqa: E402
from django.db import connection, transaction  # noqa: E402
from django.db.models import Count, F, Q, Sum  # noqa: E402

case("makemigrations", lambda: call_command("makemigrations", "shop", verbosity=0))
case("migrate (contenttypes, auth, shop)", lambda: call_command("migrate", verbosity=0))
case("migrate again is a no-op", lambda: call_command("migrate", verbosity=0))

from shop.models import Author, Book  # noqa: E402


def orm():
    a = Author.objects.create(name="ann")
    Book.objects.bulk_create([Book(author=a, title="x", price=decimal.Decimal("9.99"), tags=["a"]),
                              Book(author=a, title="y", price=decimal.Decimal("5.01"), published=True)])
    agg = list(Author.objects.annotate(n=Count("books"), total=Sum("books__price")).values_list("name", "n", "total"))
    pub = list(Book.objects.filter(Q(published=True) | Q(price__gt=9)).order_by("-price").values_list("title", flat=True))
    Book.objects.filter(title="y").update(price=F("price") + 1)
    return agg, pub, Book.objects.get(title="y").price


case("ORM: create, bulk_create, annotate, Q, F update", orm,
     ([("ann", 2, decimal.Decimal("15.00"))], ["x", "y"], decimal.Decimal("6.01")))


def atomic_rollback():
    try:
        with transaction.atomic():
            Author.objects.create(name="tmp")
            raise RuntimeError
    except RuntimeError:
        pass
    return Author.objects.filter(name="tmp").count()


case("transaction.atomic rollback", atomic_rollback, 0)


def nested_atomic():
    with transaction.atomic():
        Author.objects.create(name="outer")
        try:
            with transaction.atomic():
                Author.objects.create(name="inner")
                raise RuntimeError
        except RuntimeError:
            pass
    return sorted(Author.objects.filter(name__in=["outer", "inner"]).values_list("name", flat=True))


case("nested atomic (savepoint) rollback", nested_atomic, ["outer"])
case("unique violation -> IntegrityError", lambda: Author.objects.create(name="ann"), expect_error=True)
case("auth user create + check_password", lambda: User.objects.create_user("bob", password="pw").check_password("pw"), True)
case("JSONField contains", lambda: Book.objects.filter(tags__contains=["a"]).count(), 1)
case("cascade delete", lambda: (Author.objects.get(name="ann").delete(), Book.objects.count())[1], 0)
case("introspection: table list", lambda: sorted(t for t in connection.introspection.table_names() if t.startswith("shop_")),
     ["shop_author", "shop_book"])
report("django")
