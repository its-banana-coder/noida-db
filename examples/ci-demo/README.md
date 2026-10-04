# CI demo

A small app's integration tests touching Postgres, MySQL, Redis, Kafka and
Elasticsearch. `.github/workflows/ci-demo.yml` runs them twice, the same
code each time: once against the official service containers, once against
noida-db, so the two can be timed side by side.

Run locally against noida-db:

```sh
npx noida-db start --only postgres,mysql,redis,kafka,elasticsearch &
pip install -r requirements.txt
pytest -q
```

Ports default to the standard ones; override with `PG_PORT`, `MYSQL_PORT`,
`REDIS_PORT`, `KAFKA_PORT`, `ES_PORT`.
