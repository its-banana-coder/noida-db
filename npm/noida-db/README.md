# noida-db

One tiny binary that speaks Postgres, MySQL, Redis, Kafka, and
Elasticsearch — so your existing drivers, ORMs, and CLIs point at it,
unchanged. No Docker Compose stack idling at 2.5–4GB, no five heavy
servers to boot before `npm run dev` works.

```sh
npm install -g noida-db
noida-db start                      # every built-in service, default ports
noida-db start --only redis,postgres
noida-db start --redis-port 6380
```

This release is **linux-x64 only** for now — the other platforms land once
the project's own cross-platform build pipeline publishes them (see the
main repo for status).

Full docs, compatibility matrix, benchmarks, and the complete list of what
works today: **https://github.com/its-banana-coder/noida-db**

MIT licensed.
