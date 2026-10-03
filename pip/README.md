# noida-db

One tiny binary that speaks Postgres, MySQL, Redis, Kafka, and
Elasticsearch — so your existing drivers, ORMs, and CLIs point at it,
unchanged.

```sh
pip install noida-db
noida-db start                      # every built-in service, default ports
noida-db start --only redis,postgres
noida-db start --redis-port 6380
```

Prebuilt for Linux (x64, arm64), macOS (Intel, Apple Silicon) and Windows
(x64).

Full docs, compatibility matrix, benchmarks, and the complete list of what
works today: **https://github.com/its-banana-coder/noida-db**

MIT licensed.
