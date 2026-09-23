# MongoDB: noida spec

- **Module:** `src/mongodb/`, Cargo feature `mongodb`, branch `svc/mongodb`
- **Port:** 27017
- **Target:** MongoDB 7.0 (maxWireVersion 21), presented as a **single-node
  replica set** named `rs0` so transactions and change streams work
- **Reference server:** not installed locally (no Docker). CI uses the
  `mongo:7.0` image started with `--replSet rs0` and initiated with
  `rs.initiate()`. Env var: `NOIDA_MONGODB_REF=host:port`.

## 1. Purpose

Apps using the official drivers (Java, Node, Python, Go, Rust, C#), ODMs
(Mongoose, Spring Data MongoDB, MongoEngine/Beanie, Prisma's MongoDB
connector) and `mongosh` work unchanged.

## 2. Wire protocol (P0)

- Message header: length, requestID, responseTo, opCode (little-endian).
- **OP_MSG (2013):** flag bits (checksumPresent, moreToCome, exhaustAllowed),
  section kind 0 (body) and kind 1 (document sequence, used by insert/update/
  delete batches). Validate CRC-32C checksums when present. Honour
  `moreToCome` (unacknowledged writes, no reply).
- **OP_QUERY (2004)** on `admin.$cmd`/`<db>.$cmd`, **only for the initial
  handshake** (`isMaster`/`ismaster`/`hello` sent by drivers before they know
  the server supports OP_MSG); reply with OP_REPLY (1). Any other OP_QUERY
  use gets the error a 7.0 server gives.
- **OP_COMPRESSED (2012):** advertise no compressors in `hello`, so drivers
  don't compress (P2: zlib/snappy/zstd).
- Use the `bson` crate for documents. Justify other dependencies against
  binary size.

## 3. Handshake, auth and server commands (P0)

- `hello` / `isMaster` / `ismaster`: `isWritablePrimary`/`ismaster: true`,
  `topologyVersion`, `maxBsonObjectSize` 16777216, `maxMessageSizeBytes`
  48000000, `maxWriteBatchSize` 100000, `localTime`,
  `logicalSessionTimeoutMinutes` 30, `connectionId`, `minWireVersion` 0,
  `maxWireVersion` 21, `readOnly` false, `setName` "rs0", `hosts`, `me`,
  `primary`, `setVersion`, `electionId`, `lastWrite`, `saslSupportedMechs`
  when `saslSupportedMechs` is requested, `speculativeAuthenticate` reply
  when requested. Drivers use `hosts`/`me`: they must be the address clients
  used to connect, or `directConnection=true` must work; follow the real
  single-node replica set behaviour exactly.
- Streaming hello / awaitable hello (`topologyVersion` + `maxAwaitTimeMS`,
  exhaustAllowed) — drivers ≥ 4.4 use it for monitoring. P0.
- Auth: SCRAM-SHA-256 and SCRAM-SHA-1 via `saslStart`/`saslContinue`,
  speculative auth in hello. Default: no auth required (like `mongod`
  without `--auth`), but auth attempts with the configured user
  (default `root`/`example`, matching common Docker compose files) succeed
  and wrong passwords fail with code 18 `AuthenticationFailed`.
- `ping`, `buildInfo` (version "7.0.14", versionArray, gitVersion, bits,
  maxBsonObjectSize, storageEngines…), `hostInfo`, `getParameter`
  (featureCompatibilityVersion "7.0" etc.), `getCmdLineOpts`,
  `whatsmyuri`, `connectionStatus`, `serverStatus` (minimal but well-formed,
  including `process`, `version`, `uptime`, `connections`), `getLog`
  (startupWarnings: empty), `replSetGetStatus` (one PRIMARY member),
  `isdbgrid` → CommandNotFound (not mongos), `listCommands`, `endSessions`,
  `killSessions`, `killAllSessions`, `startSession`, `refreshSessions`,
  `logout`, `hello` on any db.
- Unknown command: `{ok: 0, errmsg: "no such command: 'x'", code: 59,
  codeName: "CommandNotFound"}`.

## 4. Data commands

### P0 (drivers' CRUD and ODMs)
- `listDatabases` (incl. `nameOnly`, `filter`), `dropDatabase`,
  `listCollections` (incl. `filter`, `nameOnly`, `authorizedCollections`,
  cursor), `create` (options: capped (accept), validator (enforce
  `$jsonSchema` P1), `timeseries` (P2)), `drop`, `renameCollection`.
- `insert` (ordered/unordered, `writeErrors` with index/code/errmsg,
  `_id` generation with ObjectId when missing, `bypassDocumentValidation`).
- `find`: filter, projection (inclusion/exclusion, `$slice`, `$elemMatch`,
  positional `$`), sort, skip, limit, batchSize, singleBatch, hint (accept),
  collation (simple + `strength` 1/2 for case-insensitive, P1), `let`,
  `allowDiskUse` (accept), `comment`, `maxTimeMS` (accept).
- Cursors: `getMore`, `killCursors`, cursor ids, `batchSize`, idle cursor
  timeout (10 minutes), tailable cursors on capped collections (P2).
- `update` (multi, upsert with `_id` and query-derived fields, arrayFilters,
  pipeline updates (P1), replacement documents), `delete` (limit 0/1),
  `findAndModify` (new, fields, upsert, remove, sort, bypass…), `count`,
  `distinct`.
- Query operators: `$eq $ne $gt $gte $lt $lte $in $nin $and $or $nor $not
  $exists $type $regex $options $mod $all $elemMatch $size $expr` (with
  aggregation expressions), dot paths into arrays and embedded docs, array
  element matching semantics, BSON type comparison order for mixed types.
- Update operators: `$set $unset $setOnInsert $inc $mul $min $max $rename
  $currentDate $push ($each $position $slice $sort) $addToSet ($each) $pop
  $pull $pullAll`, positional `$`, `$[]`, `$[<id>]`.
- Indexes: `createIndexes`, `listIndexes`, `dropIndexes` (names by
  convention `field_1_other_-1`). **Unique indexes are enforced** (E11000
  duplicate key errors with the real message format: `E11000 duplicate key
  error collection: db.coll index: email_1 dup key: { email: "x" }`, code
  11000). TTL indexes (`expireAfterSeconds`) delete expired docs
  periodically. Compound, sparse and partial indexes are accepted and
  honoured for uniqueness; other index types (text, 2dsphere, hashed,
  wildcard) accepted and stored (text search P1).
- `aggregate` with cursor: `$match $project $addFields/$set $unset $group
  ($sum $avg $min $max $first $last $push $addToSet $count) $sort $limit $skip
  $count $unwind $lookup (localField/foreignField and pipeline form)
  $replaceRoot/$replaceWith $facet $bucket $sample $out $merge $sortByCount
  $group with _id null`, and the common expression operators (arithmetic,
  comparison, boolean, `$cond $ifNull $switch`, string `$concat $substr
  $toLower $toUpper $split $regexMatch`, array `$size $arrayElemAt $filter
  $map $in $slice`, date `$dateToString $year $month $dayOfMonth $hour`,
  type `$toString $toInt $toDate $convert`).
- Write concern / read concern / read preference fields accepted; `w`
  values > 1 behave as `majority` on one node.

### P1
Transactions (`startTransaction` via `autocommit: false` + `txnNumber` on
sessions, `commitTransaction`, `abortTransaction`, snapshot isolation on one
node, `TransientTransactionError` labels where real MongoDB adds them),
retryable writes (`txnNumber` dedup), **change streams** (`$changeStream`
stage on collection/db/cluster, resume tokens, `fullDocument:
updateLookup`), `$jsonSchema` validation, `text` indexes + `$text`,
`collStats`/`dbStats` (minimal numbers), `explain` (minimal
queryPlanner-shaped reply; no executionStats analysis), GridFS (works on
top of normal collections; verify with drivers' GridFS APIs), `$graphLookup
$unionWith $densify $fill $setWindowFields`, collation.

### P2
Capped collections with tailable cursors, time-series collections,
geospatial queries, `mapReduce` (deprecated) → error as 7.0 gives for
unsupported JS, `$where`/`$function`/`$accumulator` (server-side JS: return
the error a server with JS disabled gives), users/roles commands, sharding
commands (reply as a replica set member, not mongos).

## 5. Error model

Every error reply has `ok: 0`, `errmsg`, `code`, `codeName` matching
MongoDB 7.0 for the same situation: 2 BadValue, 9 FailedToParse, 11000
DuplicateKey, 13 Unauthorized, 18 AuthenticationFailed, 26
NamespaceNotFound, 43 CursorNotFound, 48 NamespaceExists, 59
CommandNotFound, 66 ImmutableField, 72 InvalidOptions, 112 WriteConflict,
121 DocumentValidationFailure, 251 NoSuchTransaction, 263
OperationNotSupportedInTransaction… Write errors go in `writeErrors`, not
top-level failure, as MongoDB does.

## 6. Storage and behaviour

- Databases → collections → documents stored as BSON. Field order is
  preserved exactly (drivers and tests compare documents including order).
- Natural order (no sort) = insertion order, like a fresh WiredTiger
  collection for simple cases; tests that depend on natural order should
  still be compared against real MongoDB.
- `_id` immutable; ObjectId generation: 4-byte timestamp, 5-byte process
  random, 3-byte counter.
- One lock; in-memory acceptable for the first milestone, then persistence
  to the data dir.

## 7. Client matrix

Scenario: connect (with and without SCRAM credentials), insert/find with
filters and projections, update operators, upsert, unique index violation,
aggregation with `$group` + `$lookup`, cursor iteration past the first
batch, delete; P1: a transaction and a change stream.

| Client | How to run |
|---|---|
| Rust `mongodb` crate | dev-dependency in `tests/mongodb_client.rs` |
| pymongo | pip |
| Node driver + Mongoose | npm |
| Java driver (sync) | jars from Maven Central, `javac` |
| `mongosh` | npm package `mongosh` (optional) |
| Spring Data MongoDB, Go driver | P1, CI |

Commit test apps under `tests/clients/mongodb/` with a runner script.

## 8. Differential tests

`tests/mongodb_diff.rs` runs command sequences (as BSON documents) against
real MongoDB and noida and compares replies with normalization only for
values that legitimately differ (`$clusterTime`, `operationTime`,
`electionId`, `localTime`, connection ids, ObjectIds generated server-side,
cursor ids). Cover every P0 command and operator, including error replies.
CI: `mongo:7.0` service initiated as a replica set. Print the number of
compared replies.

## 9. Non-goals

Sharding/mongos, real replication and elections, Atlas features (search,
vector search, triggers), server-side JavaScript, encryption (CSFLE/QE),
auditing, `$currentOp`/profiler/`serverStatus` performance metrics.

## 10. Milestones

1. OP_QUERY handshake + OP_MSG + hello/buildInfo/ping; `mongosh` and pymongo
   connect.
2. CRUD (insert/find/getMore/update/delete/findAndModify) with query and
   update operators; unique indexes; diff suite for these.
3. Aggregation P0 stages; driver matrix green.
4. Auth (SCRAM), streaming hello; `mongodb` in default features.
5. P1: transactions, change streams, validation, GridFS.
