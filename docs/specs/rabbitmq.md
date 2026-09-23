# RabbitMQ: noida spec

- **Module:** `src/rabbitmq/`, Cargo feature `rabbitmq`, branch
  `svc/rabbitmq`
- **Ports:** 5672 (AMQP 0-9-1), 15672 (management HTTP API)
- **Target:** RabbitMQ 3.13 (product "RabbitMQ", version "3.13.7" in server
  properties)
- **Reference server:** not installed locally (no Docker). CI uses
  `rabbitmq:3.13-management`. Env vars: `NOIDA_RABBITMQ_REF=host:5672`,
  `NOIDA_RABBITMQ_MGMT_REF=host:15672`.

## 1. Purpose

Apps using AMQP 0-9-1 (Spring AMQP/Spring Boot, the Java `amqp-client`,
pika, aio-pika, amqplib, Celery, MassTransit/.NET, Go amqp091-go, PHP
php-amqplib, Ruby bunny) and tools that use the management API work
unchanged. Default vhost `/`, default user `guest`/`guest`.

## 2. AMQP 0-9-1 protocol (P0)

- Protocol header `AMQP\x00\x00\x09\x01`; a different header gets the
  supported header back and the socket closed, as RabbitMQ does.
- Frames: method (1), content header (2), body (3), heartbeat (8); frame end
  0xCE; `frame_max` negotiation (default 131072), `channel_max` (2047),
  heartbeats (default 60s; honour client value; close dead connections).
- Field tables and all AMQP types (incl. RabbitMQ's extensions: signed 8/16
  types, `x` byte arrays, decimals, timestamps, nested tables and arrays).
- **Connection:** Start (server properties incl. `capabilities`:
  `publisher_confirms`, `exchange_exchange_bindings`, `basic.nack`,
  `consumer_cancel_notify`, `connection.blocked`,
  `consumer_priorities`, `authentication_failure_close`,
  `per_consumer_qos`, `direct_reply_to`; `cluster_name`, `product`,
  `version`, `platform`, `copyright`, `information`; mechanisms
  `PLAIN AMQPLAIN`; locales `en_US`), StartOk, Secure/SecureOk (unused),
  Tune/TuneOk, Open/OpenOk (vhost must exist: else close 530
  `NOT_ALLOWED - vhost x not found`), Close/CloseOk, Blocked/Unblocked
  (never sent unless the memory cap is hit), UpdateSecret.
- **Channel:** Open/OpenOk, Flow/FlowOk, Close/CloseOk.
- **Exchange:** Declare (types `direct`, `fanout`, `topic`, `headers`;
  passive; durable; auto-delete; internal; arguments incl.
  `alternate-exchange`), Delete (if-unused), Bind/Unbind (exchange-to-
  exchange). Built-ins: default exchange `""`, `amq.direct`, `amq.fanout`,
  `amq.topic`, `amq.headers`, `amq.match`, `amq.rabbitmq.trace`
  (declaring/deleting `amq.*` → 403 `ACCESS_REFUSED`).
- **Queue:** Declare (passive, durable, exclusive, auto-delete, server-named
  `amq.gen-…` names, arguments below), Bind, Unbind, Purge, Delete (if-unused,
  if-empty). Reply with message and consumer counts.
- **Basic:** Qos (prefetch count per consumer or per channel with
  `global`), Consume (consumer tags, no-local accepted, no-ack, exclusive,
  arguments incl. `x-priority`), Cancel, Publish (mandatory → Return with
  312 `NO_ROUTE`; immediate → connection error 540 as RabbitMQ does),
  Deliver, Get/GetOk/GetEmpty, Ack/Nack/Reject (single and `multiple`),
  Recover/RecoverAsync, consumer cancel notifications when a queue is
  deleted.
- **Confirm.Select** (publisher confirms: acks with delivery tags,
  `multiple`, nacks when a message can't be enqueued) and **Tx**
  Select/Commit/Rollback.
- Content header properties: content-type, content-encoding, headers,
  delivery-mode, priority, correlation-id, reply-to, expiration, message-id,
  timestamp, type, user-id (validated against the connection's user as
  RabbitMQ does), app-id.

### Queue arguments and features (P0 unless noted)
`x-message-ttl`, per-message `expiration`, `x-expires`, `x-max-length`,
`x-max-length-bytes`, `x-overflow` (`drop-head`, `reject-publish`,
`reject-publish-dlx`), `x-dead-letter-exchange` and
`x-dead-letter-routing-key` (dead-lettering on reject/nack requeue=false,
TTL expiry and max-length, adding the `x-death` header exactly as RabbitMQ
does), `x-max-priority` (priority queues), `x-single-active-consumer`,
`x-queue-type` (`classic`; `quorum` and `stream` are accepted and behave as
classic queues for local use, with quorum-specific args like
`x-delivery-limit` honoured, P1), **direct reply-to**
(`amq.rabbitmq.reply-to`, P1), `x-queue-mode` (accepted).

### Semantics that must match
- Routing: direct (exact key), fanout, topic (`*` one word, `#` zero or more,
  dot-separated), headers (`x-match` `all`/`any`, plus `all-with-x`/
  `any-with-x`).
- Delivery order per queue is FIFO (priority queues: highest first);
  unacked messages on channel/connection close are requeued with
  `redelivered=true`.
- Round-robin between consumers on a queue, respecting prefetch;
  single-active-consumer and consumer priorities.
- Exclusive queues are deleted when their connection closes; auto-delete
  queues when their last consumer cancels; auto-delete exchanges when their
  last binding goes.
- Errors are channel or connection closes with RabbitMQ's exact reply codes
  and texts, e.g. 404 `NOT_FOUND - no queue 'q' in vhost '/'`, 406
  `PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'q' in vhost
  '/': received 'true' but current is 'false'`, 405 `RESOURCE_LOCKED -
  cannot obtain exclusive access to locked queue 'q' in vhost '/'…`, 406
  `PRECONDITION_FAILED - unknown delivery tag 5`, 403 `ACCESS_REFUSED -
  Login was refused using authentication mechanism PLAIN…`, 504
  `CHANNEL_ERROR - expected 'channel.open'`, 505 `UNEXPECTED_FRAME`, 530
  `NOT_ALLOWED`, 540 `NOT_IMPLEMENTED`.

## 3. Management HTTP API (port 15672)

Basic auth (`guest`/`guest`), JSON. P0 endpoints (what Spring Boot actuator,
rabbitmqadmin, Testcontainers-style health checks and common scripts use):
`GET /api/overview`, `/api/whoami`, `/api/vhosts`, `/api/vhosts/{vhost}`
(PUT/DELETE P1), `/api/queues`, `/api/queues/{vhost}`,
`/api/queues/{vhost}/{name}` (GET/PUT/DELETE), `/api/queues/{vhost}/{name}/
contents` (DELETE = purge), `/api/queues/{vhost}/{name}/get` (POST),
`/api/exchanges[/{vhost}[/{name}]]` (GET/PUT/DELETE),
`/api/exchanges/{vhost}/{name}/publish` (POST), `/api/bindings[...]`,
`/api/bindings/{vhost}/e/{exchange}/q/{queue}` (GET/POST),
`/api/connections`, `/api/channels`, `/api/consumers`, `/api/users` (GET),
`/api/permissions`, `/api/nodes` (one node), `/api/aliveness-test/{vhost}`,
`/api/health/checks/alarms`, `/api/health/checks/local-alarms`,
`/api/health/checks/virtual-hosts`, `/api/definitions` (GET export, POST
import, P1). Field names and shapes match 3.13 (stats fields may be 0; no
rate/performance analysis). The web UI itself is a non-goal
(`GET /` may return a small page saying so).

## 4. Storage

Queues are in-memory FIFO structures; durable entities and persistent
messages survive restart once project-wide persistence lands (in-memory is
fine for the first milestone). One lock; a thread per connection.

## 5. Client matrix

Scenarios: (a) declare topology (topic exchange, 2 queues, bindings),
publish with confirms, consume with manual ack and prefetch 10; (b) RPC
over direct reply-to; (c) dead-lettering on reject and on TTL; (d)
mandatory publish to an unroutable key → return; (e) passive declare of a
missing queue → 404 channel close; (f) management API lists the queue with
its message count.

| Client | How to run |
|---|---|
| Rust `lapin` | dev-dependency in `tests/rabbitmq_client.rs` |
| pika | pip |
| amqplib (Node) | npm |
| Java `amqp-client` 5.x | jar from Maven Central, `javac` |
| Celery (with pika/kombu) | pip, P1 |
| Spring AMQP, .NET, Go | P1/P2, CI |

Commit test apps under `tests/clients/rabbitmq/` with a runner script.

## 6. Differential tests

`tests/rabbitmq_diff.rs` runs frame-level scenarios against real RabbitMQ
and noida and compares decoded methods, properties, bodies and close
codes/texts, normalizing server-generated names (`amq.gen-*`, consumer
tags), timestamps and server properties' version details. A second suite
compares management API JSON for the same state (ignoring stats and node
names). CI: `rabbitmq:3.13-management` service. Print the number of
compared frames/responses.

## 7. Non-goals

Clustering, federation, shovel, plugins (other than the management API
subset above), streams protocol (port 5552), MQTT/STOMP/AMQP 1.0, TLS,
policies and operator policies (P2: accept and store), memory/disk alarms
(except honouring noida's memory cap), the management web UI.

## 8. Milestones

1. Connection/channel handshake, heartbeats, exchange/queue declare/bind;
   pika connects and declares topology.
2. Publish/consume/get/ack/nack/reject/qos with all four exchange types;
   diff suite for these.
3. Confirms, tx, mandatory/returns, TTL, dead-lettering, max-length,
   priority, exclusive/auto-delete; client matrix green.
4. Management API P0; `rabbitmq` in default features.
5. P1: direct reply-to, quorum args, definitions import/export, Celery.
