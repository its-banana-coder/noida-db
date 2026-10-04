"""A small app's integration tests, touching all five services the way a
typical web app does. CI runs the same file against real service containers
and against noida-db (see .github/workflows/ci-demo.yml)."""
import os
import time
import uuid

import psycopg
import pymysql
import redis
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from elasticsearch import Elasticsearch

HOST = "127.0.0.1"
PORT = {k: int(os.environ.get(f"{k.upper()}_PORT", d)) for k, d in
        {"pg": 5432, "mysql": 3306, "redis": 6379, "kafka": 9092, "es": 9200}.items()}
PG = f"host={HOST} port={PORT['pg']} user=postgres password=postgres dbname=postgres"
KAFKA = f"{HOST}:{PORT['kafka']}"


def mysql():
    return pymysql.connect(host=HOST, port=PORT["mysql"], user="root", password="", database="test", autocommit=True)


def test_postgres_orders_flow():
    with psycopg.connect(PG, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS orders")
        c.execute("CREATE TABLE orders (id serial PRIMARY KEY, customer text NOT NULL, total numeric(10,2), placed timestamptz DEFAULT now())")
        c.execute("INSERT INTO orders (customer, total) VALUES ('ada', 19.99), ('ada', 5.01), ('linus', 42.00)")
        rows = c.execute("SELECT customer, sum(total) FROM orders GROUP BY customer ORDER BY customer").fetchall()
        assert [(r[0], str(r[1])) for r in rows] == [("ada", "25.00"), ("linus", "42.00")]


def test_postgres_transaction_rollback():
    with psycopg.connect(PG) as c:
        c.execute("CREATE TABLE IF NOT EXISTS accounts (id int PRIMARY KEY, balance int)")
        c.execute("DELETE FROM accounts")
        c.execute("INSERT INTO accounts VALUES (1, 100)")
        c.commit()
        c.execute("UPDATE accounts SET balance = balance - 50 WHERE id = 1")
        c.rollback()
        assert c.execute("SELECT balance FROM accounts WHERE id = 1").fetchone()[0] == 100


def test_mysql_users_and_upsert():
    with mysql() as c, c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS users")
        cur.execute("CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(100) UNIQUE, visits INT DEFAULT 0)")
        cur.execute("INSERT INTO users (email) VALUES (%s), (%s)", ("a@x.io", "b@x.io"))
        cur.execute("INSERT INTO users (email, visits) VALUES (%s, 1) ON DUPLICATE KEY UPDATE visits = visits + 1", ("a@x.io",))
        cur.execute("SELECT email, visits FROM users ORDER BY id")
        assert cur.fetchall() == (("a@x.io", 1), ("b@x.io", 0))


def test_mysql_join_report():
    with mysql() as c, c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS items")
        cur.execute("DROP TABLE IF EXISTS categories")
        cur.execute("CREATE TABLE categories (id INT PRIMARY KEY, name VARCHAR(20))")
        cur.execute("CREATE TABLE items (id INT PRIMARY KEY, category_id INT, price DECIMAL(8,2))")
        cur.execute("INSERT INTO categories VALUES (1, 'books'), (2, 'games')")
        cur.execute("INSERT INTO items VALUES (1, 1, 10.00), (2, 1, 15.50), (3, 2, 60.00)")
        cur.execute("SELECT c.name, COUNT(*), SUM(i.price) FROM categories c JOIN items i ON i.category_id = c.id GROUP BY c.name ORDER BY c.name")
        assert [(n, k, str(s)) for n, k, s in cur.fetchall()] == [("books", 2, "25.50"), ("games", 1, "60.00")]


def test_redis_cache_and_counter():
    r = redis.Redis(host=HOST, port=PORT["redis"])
    r.set("session:1", "ada", ex=60)
    assert r.get("session:1") == b"ada"
    assert 0 < r.ttl("session:1") <= 60
    r.delete("hits")
    assert [r.incr("hits") for _ in range(3)] == [1, 2, 3]
    r.hset("user:1", mapping={"name": "ada", "plan": "pro"})
    assert r.hgetall("user:1") == {b"name": b"ada", b"plan": b"pro"}


def test_kafka_produce_consume():
    topic = f"events-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": KAFKA})
    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(30)
    p = Producer({"bootstrap.servers": KAFKA})
    for i in range(5):
        p.produce(topic, key=b"order", value=f"placed-{i}".encode())
    p.flush(30)
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"g-{topic}", "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    got, deadline = [], time.time() + 30
    while len(got) < 5 and time.time() < deadline:
        m = c.poll(0.5)
        if m is not None and not m.error():
            got.append(m.value().decode())
    c.close()
    assert got == [f"placed-{i}" for i in range(5)]


def test_elasticsearch_search():
    es = Elasticsearch(f"http://{HOST}:{PORT['es']}")
    idx = f"products-{uuid.uuid4().hex[:8]}"
    es.indices.create(index=idx, mappings={"properties": {"name": {"type": "text"}, "tags": {"type": "keyword"}, "price": {"type": "float"}}})
    docs = [("1", "red running shoes", ["shoes"], 80.0), ("2", "blue running jacket", ["apparel"], 120.0), ("3", "red hat", ["apparel"], 15.0)]
    for i, name, tags, price in docs:
        es.index(index=idx, id=i, document={"name": name, "tags": tags, "price": price})
    es.indices.refresh(index=idx)
    hits = es.search(index=idx, query={"bool": {"must": {"match": {"name": "running"}}, "filter": {"range": {"price": {"lt": 100}}}}})
    assert [h["_id"] for h in hits["hits"]["hits"]] == ["1"]
    aggs = es.search(index=idx, size=0, aggs={"t": {"terms": {"field": "tags"}}})
    assert {b["key"]: b["doc_count"] for b in aggs["aggregations"]["t"]["buckets"]} == {"apparel": 2, "shoes": 1}
