# noida-db control API (v1) — shared contract between `noida-db up/status/...` and `noida-db ui`

Discovery: the running server writes `<data_dir>/noida.control.json`:
  {"pid": 1234, "port": 51234, "token": "<random 32 hex>", "version": "0.2.2", "started_ms": 1760000000000}
It binds the control port on 127.0.0.1 only (port 0 = OS-assigned). The file is removed on clean shutdown.
A stale file (pid dead, or the port refuses) means "not running". Clients find it via --data-dir (default ./.noida-db).

Every request carries the header `Authorization: Bearer <token>`. HTTP/1.1, JSON bodies, `Connection: close` is fine.
Errors: 4xx/5xx with {"error": "<message>"}. 401 without the token, 404 for an unknown service.

GET  /v1/status ->
  {"version": "0.2.2", "pid": 1234, "data_dir": "/abs/path", "uptime_ms": 12345,
   "rss_bytes": 12345678 | null,
   "services": [
     {"name": "postgres", "state": "running" | "stopped" | "unavailable",
      "host": "127.0.0.1", "port": 5432,
      "connect": "postgres://postgres@127.0.0.1:5432/postgres",
      "data_bytes": 4096 | null,
      "error": null | "<last start error, e.g. port in use>"}
   ]}
   ("unavailable" = not compiled into this build.) Services are listed in the fixed order
   postgres, mysql, redis, kafka, elasticsearch, clickhouse, mongodb.

POST /v1/services/{name}/start    -> {"service": {...same object as in status...}}
POST /v1/services/{name}/stop     -> {"service": {...}}   (saves its data, closes its connections, frees its memory)
POST /v1/services/{name}/restart  -> {"service": {...}}
POST /v1/services/{name}/reset    -> {"service": {...}}   (wipes that service's saved data; leaves it in the state it was in)
POST /v1/shutdown                 -> {"ok": true}         (stops everything, saves, exits the process)

GET  /v1/activity?since=<seq>&limit=<n, default 200, max 1000> ->
  {"next": <seq to pass as `since` next time>,
   "events": [{"seq": 17, "ts_ms": 1760000000123, "service": "postgres", "text": "CREATE TABLE users (id int)"}]}
  Kept in an in-memory ring buffer (about 1000 events). `text` is a one-line summary truncated to about 200 chars:
  the SQL statement for postgres/mysql, the command and first args for redis, the API name and topic for kafka,
  the method and path for elasticsearch. Statement text is shown as sent, with no secrets added. Login passwords are never included.

Connection strings (the `connect` field):
  postgres       postgres://postgres@HOST:PORT/postgres
  mysql          mysql://root@HOST:PORT
  redis          redis://HOST:PORT
  kafka          HOST:PORT
  elasticsearch  http://HOST:PORT
  clickhouse     http://HOST:PORT
  mongodb        mongodb://HOST:PORT
