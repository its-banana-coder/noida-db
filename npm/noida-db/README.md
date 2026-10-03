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

Or without installing: `npx noida-db start`.

Prebuilt for Linux (x64, arm64), macOS (Intel, Apple Silicon) and Windows
(x64); npm installs the right one automatically.

Full docs, compatibility matrix, benchmarks, and the complete list of what
works today: **https://github.com/its-banana-coder/noida-db**

MIT licensed.
