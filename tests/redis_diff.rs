//! Differential tests: every script runs against real Redis and noida, and
//! every reply must be identical.
//!
//! The reference server is `NOIDA_REDIS_REF=host:port` if set (CI points this
//! at Redis 7.2), otherwise a `redis-server` from PATH started on a free port.
//! Scripts newer than the reference server's version are skipped and counted.

mod common;

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::RawClient;
use noida::redis::resp::Value;

/// (minimum Redis version, script). Line prefixes:
/// - `~` compares the reply as an unordered set (KEYS and similar).
/// - `@X.Y ` compares only if the reference is at least X.Y (error wording
///   that changed since older Redis). The command still runs on both.
/// - `!` runs the command on both but doesn't compare (ids, versions).
/// - `&N CMD` sends CMD on extra connection N (1-9) without reading the
///   reply (a blocking command), then waits a moment so it is seen first.
/// - `<N` reads the next reply on connection N and compares it.
/// - `=N CMD` runs CMD on connection N and compares the reply.
const SCRIPTS: &[((u32, u32), &[&str])] = &[
    ((2, 0), &["PING", "PING hi", "ECHO x", "PING a b", "ECHO"]),
    ((2, 0), &["GET k", "SET k v", "GET k", "SET k w", "GET k", "GET", "SET k"]),
    (
        (2, 6),
        &[
            "SET k v XX",
            "SET k v NX",
            "SET k w NX",
            "SET k w XX",
            "GET k",
            "SET k v NX XX",
            "SET k v BOGUS",
        ],
    ),
    (
        (2, 6),
        &[
            "SET k v EX 100",
            "TTL k",
            "SET k v PX 5000",
            "TTL k",
            "SET k v",
            "TTL k",
            "@7.0 SET k v EX 0",
            "SET k v EX abc",
            "SET k v EX 1 PX 1",
            "SET k v EX",
        ],
    ),
    ((6, 0), &["SET k v EX 100", "SET k v2 KEEPTTL", "TTL k", "SET k v3 EX 5 KEEPTTL"]),
    (
        (6, 2),
        &[
            "SET k a",
            "SET k b GET",
            "SET nope b GET",
            "GETDEL k",
            "GETDEL k",
            "SET k v",
            "GETEX k EX 50",
            "TTL k",
            "GETEX k PERSIST",
            "TTL k",
            "GETEX k EX 1 PX 1",
            "GETEX k EX 0",
            "GETEX missing",
        ],
    ),
    (
        (2, 0),
        &[
            "SETNX k a",
            "SETNX k b",
            "GETSET k c",
            "GET k",
            "SETEX k 30 v",
            "TTL k",
            "@7.0 SETEX k 0 v",
            "SETEX k x v",
            "@7.0 PSETEX k 0 v",
        ],
    ),
    (
        (2, 0),
        &[
            "MSET a 1 b 2",
            "MGET a b c",
            "@7.0 MSET a 1 b",
            "MSETNX c 3 a 9",
            "GET c",
            "MSETNX c 3 d 4",
            "MGET c d",
        ],
    ),
    (
        (2, 0),
        &[
            "INCR n",
            "INCRBY n 10",
            "DECR n",
            "DECRBY n 4",
            "GET n",
            "SET n 9223372036854775807",
            "INCR n",
            "SET s abc",
            "INCR s",
            "SET s 007",
            "INCR s",
            "SET s +1",
            "INCR s",
            "INCRBY n x",
            "SET n -9223372036854775808",
            "DECR n",
            "@7.0 DECRBY n -9223372036854775808",
        ],
    ),
    (
        (2, 6),
        &[
            "SET f 10.50",
            "INCRBYFLOAT f 0.1",
            "INCRBYFLOAT f -5",
            "SET f 5.0e3",
            "INCRBYFLOAT f 2.0e2",
            "INCRBYFLOAT new 3",
            "SET g 0.1",
            "INCRBYFLOAT g 0.2",
            "SET s abc",
            "INCRBYFLOAT s 1",
            "INCRBYFLOAT f x",
            "INCRBYFLOAT f inf",
            "SET t 1000",
            "INCRBYFLOAT t 0.1",
            "INCRBYFLOAT t 1e300",
            "SET u 0.1",
            "INCRBYFLOAT u 0.2",
        ],
    ),
    (
        (2, 2),
        &[
            "APPEND k Hello",
            "APPEND k World",
            "STRLEN k",
            "STRLEN missing",
            "SET s Thisisastring",
            "GETRANGE s 0 3",
            "GETRANGE s -3 -1",
            "GETRANGE s 0 -1",
            "GETRANGE s 10 100",
            "GETRANGE s 5 2",
            "GETRANGE s -1 -5",
            "GETRANGE missing 0 -1",
            "SUBSTR s 0 3",
            "SETRANGE s 4 XX",
            "GET s",
            "SETRANGE z 3 ab",
            "GET z",
            "SETRANGE s -1 x",
            "GETRANGE s a 1",
        ],
    ),
    (
        (7, 0),
        &[
            "MSET key1 ohmytext key2 mynewtext",
            "LCS key1 key2",
            "LCS key1 key2 LEN",
            "LCS key1 key2 IDX",
            "LCS key1 key2 IDX MINMATCHLEN 4 WITHMATCHLEN",
            "LCS key1 key2 LEN IDX",
            "LCS key1 key2 BOGUS",
            "LCS missing1 missing2",
        ],
    ),
    (
        (2, 0),
        &[
            "MSET a 1 b 2",
            "EXISTS a b missing a",
            "TYPE a",
            "TYPE missing",
            "DEL a missing",
            "UNLINK b",
            "EXISTS a b",
            "TOUCH a",
        ],
    ),
    (
        (2, 0),
        &[
            "TTL k",
            "EXPIRE k 10",
            "SET k v",
            "TTL k",
            "EXPIRE k 100",
            "TTL k",
            "PEXPIRE k 50000",
            "TTL k",
            "PERSIST k",
            "PERSIST k",
            "TTL k",
            "EXPIRE k abc",
            "EXPIRE k -1",
            "EXISTS k",
            "SET k v",
            "EXPIREAT k 1",
            "EXISTS k",
        ],
    ),
    (
        (7, 0),
        &[
            "SET k v",
            "EXPIRE k 10 XX",
            "EXPIRE k 10 NX",
            "EXPIRE k 20 NX",
            "EXPIRE k 5 GT",
            "EXPIRE k 50 GT",
            "EXPIRE k 60 LT",
            "EXPIRE k 30 LT",
            "TTL k",
            "EXPIRE k 30 NX XX",
            "EXPIRE k 30 GT LT",
            "EXPIRE k 30 FOO",
            "SET p v",
            "EXPIRE p 10 GT",
            "EXPIRE p 10 LT",
            "EXPIRETIME missing",
            "PERSIST p",
            "EXPIRETIME p",
        ],
    ),
    (
        (2, 0),
        &[
            "MSET hello 1 hallo 1 hxllo 1 hllo 1 heeeello 1 h*llo 1",
            "~KEYS h?llo",
            "~KEYS h*llo",
            "~KEYS h[ae]llo",
            "~KEYS h[^e]llo",
            "~KEYS h[a-b]llo",
            "KEYS h\\*llo",
            "~KEYS *",
            "KEYS nomatch*",
        ],
    ),
    (
        (2, 0),
        &[
            "RENAME a b",
            "SET a 1",
            "EXPIRE a 100",
            "SET c 3",
            "RENAME a b",
            "GET b",
            "TTL b",
            "RENAMENX b c",
            "RENAMENX b d",
            "RENAME d d",
            "RENAMENX d d",
            "RENAMENX missing x",
        ],
    ),
    (
        (6, 2),
        &[
            "SET a 1",
            "COPY a b",
            "COPY a b",
            "COPY a b REPLACE",
            "COPY a x DB 3",
            "COPY a a",
            "COPY a b DB 99",
            "COPY a b BOGUS",
            "COPY missing z",
        ],
    ),
    (
        (2, 0),
        &[
            "SET a 1",
            "MOVE a 2",
            "EXISTS a",
            "MOVE a 2",
            "MOVE missing 2",
            "SET b 1",
            "MOVE b 0",
            "@7.0 MOVE b 99",
            "@7.0 MOVE b x",
            "SELECT 2",
            "GET a",
            "SELECT 0",
        ],
    ),
    (
        (2, 0),
        &[
            "SET k v0",
            "SELECT 1",
            "GET k",
            "SET k v1",
            "DBSIZE",
            "SELECT 0",
            "GET k",
            "@7.0 SELECT 16",
            "@7.0 SELECT x",
            "@7.0 SELECT -1",
        ],
    ),
    (
        (4, 0),
        &[
            "SET a 1",
            "SELECT 1",
            "SET b 2",
            "SWAPDB 0 1",
            "GET b",
            "SELECT 0",
            "GET b",
            "SWAPDB 0 99",
            "SWAPDB x 1",
            "SWAPDB 0 y",
        ],
    ),
    (
        (4, 0),
        &[
            "SET a 1",
            "FLUSHDB",
            "DBSIZE",
            "SET a 1",
            "FLUSHALL ASYNC",
            "DBSIZE",
            "FLUSHALL LAZY",
            "FLUSHDB SYNC ASYNC",
        ],
    ),
    (
        (2, 8),
        &[
            "SET a 1",
            "SCAN 0 COUNT 100",
            "SCAN x",
            "SCAN 0 COUNT 0",
            "SCAN 0 COUNT x",
            "SCAN 0 BOGUS 1",
        ],
    ),
    ((7, 0), &["FOO a b", "foo"]),
    (
        (4, 0),
        &[
            "HSET h a 1 b 2",
            "HSET h a 9 c 3",
            "HGET h a",
            "HGET h nope",
            "HGET nokey a",
            "HMSET h x 1 y 2",
            "HGETALL h",
            "HKEYS h",
            "HVALS h",
            "HDEL h a nope b",
            "HGETALL h",
            "HLEN h",
            "HDEL h c x y",
            "EXISTS h",
            "HDEL h a",
            "HSET h a",
            "@7.0 HMSET h x",
        ],
    ),
    (
        (4, 0),
        &[
            "HSETNX h f v",
            "HSETNX h f w",
            "HEXISTS h f",
            "HEXISTS h g",
            "HEXISTS nokey f",
            "HLEN nokey",
            "HSET h long helloworld",
            "HSTRLEN h long",
            "HSTRLEN h nope",
            "HMGET h f nope long",
            "HMGET nokey a",
            "HGETALL nokey",
            "HKEYS nokey",
            "HVALS nokey",
        ],
    ),
    ((4, 0), &["HSET h z 1 a 2 m 3", "HSET h a 20", "HDEL h z", "HGETALL h", "HKEYS h", "HVALS h"]),
    (
        (4, 0),
        &[
            "HINCRBY h n 5",
            "HINCRBY h n -7",
            "HINCRBY h n x",
            "HSET h s abc",
            "HINCRBY h s 1",
            "HSET h big 9223372036854775807",
            "HINCRBY h big 1",
            "HSET h f 10.50",
            "HINCRBYFLOAT h f 0.1",
            "HINCRBYFLOAT h new 0.25",
            "HINCRBYFLOAT h n 1.5",
            "HINCRBYFLOAT h f x",
            "HINCRBYFLOAT h s 1",
            "HGETALL h",
        ],
    ),
    (
        (7, 0),
        &[
            "HSET h f 1",
            "HINCRBYFLOAT h f inf",
            "HSET h huge 1e308",
            "HINCRBYFLOAT h huge 1e308",
            "HSET h max 1e4932",
            "HINCRBYFLOAT h max 1e4932",
            "SET s 1000",
            "INCRBYFLOAT s 0.1",
        ],
    ),
    (
        (4, 0),
        &[
            "HSET h f v",
            "SET s v",
            "TYPE h",
            "GET h",
            "APPEND h x",
            "INCR h",
            "HGET s f",
            "HSET s f v",
            "HGETALL s",
            "MGET h s",
            "SET h str",
            "TYPE h",
        ],
    ),
    (
        (4, 0),
        &[
            "HSET h a 1 b 2 c 3",
            "HSCAN h 0 COUNT 1",
            "HSCAN h 0 MATCH b*",
            "HSCAN nokey 0",
            "HSCAN h x",
            "HSCAN h 0 COUNT 0",
            "HSCAN h 0 TYPE string",
        ],
    ),
    (
        (6, 2),
        &[
            "HRANDFIELD nokey",
            "HRANDFIELD nokey 3",
            "HSET h a 1 b 2 c 3",
            "HRANDFIELD h 0",
            "HRANDFIELD h 5",
            "HRANDFIELD h 3 WITHVALUES",
            "HRANDFIELD h 1 FOO",
            "HRANDFIELD h x",
            "@7.0 HRANDFIELD h -9223372036854775807 WITHVALUES",
            "!HELLO 3",
            "HRANDFIELD h 3 WITHVALUES",
            "HGETALL h",
        ],
    ),
    ((6, 0), &["!HELLO 3", "SET k v", "GET k", "MGET k nope", "GET nope", "!HELLO 2", "GET nope"]),
    ((7, 0), &["HELLO 4", "HELLO x", "HELLO 3 FOO", "HELLO 3 SETNAME", "HELLO 3 AUTH bob pw"]),
    ((6, 0), &["AUTH secret", "@6.2 AUTH default whatever", "@7.0 AUTH bob pw", "AUTH a b c"]),
    (
        (6, 0),
        &[
            "CLIENT GETNAME",
            "CLIENT SETNAME app",
            "CLIENT GETNAME",
            "@7.0 CLIENT SETNAME",
            "CLIENT",
            "@7.0 CLIENT FOO",
            "@7.2 CLIENT HELP",
        ],
    ),
    (
        (7, 2),
        &[
            "CLIENT SETINFO lib-name mylib",
            "CLIENT SETINFO lib-ver 1.0",
            "CLIENT SETINFO foo x",
            "CLIENT NO-EVICT on",
            "CLIENT NO-EVICT off",
            "CLIENT NO-TOUCH maybe",
            "CLIENT NO-EVICT maybe",
        ],
    ),
    (
        (6, 2),
        &[
            "CLIENT KILL 1.2.3.4:5",
            "CLIENT KILL ID 0",
            "CLIENT KILL ID 99999",
            "CLIENT KILL TYPE foo",
            "CLIENT KILL USER bob",
            "CLIENT KILL SKIPME maybe",
            "CLIENT LIST TYPE foo",
            "CLIENT LIST FOO",
            "CLIENT LIST ID x",
        ],
    ),
    (
        (6, 2),
        &[
            "CLIENT UNBLOCK 99999",
            "CLIENT UNBLOCK 99999 FOO",
            "CLIENT PAUSE 0 FOO",
            "CLIENT PAUSE x",
            "CLIENT PAUSE -1",
            "CLIENT PAUSE 0",
            "CLIENT UNPAUSE",
        ],
    ),
    ((6, 2), &["SELECT 3", "RESET", "GET k"]),
    (
        (7, 0),
        &[
            "COMMAND HELP",
            "COMMAND GETKEYS SET k v",
            "COMMAND GETKEYS MSET a 1 b 2",
            "COMMAND GETKEYS PING",
            "COMMAND GETKEYS NOSUCH x",
            "COMMAND GETKEYS GET",
            "COMMAND GETKEYSANDFLAGS SET k v",
            "COMMAND GETKEYSANDFLAGS SET k v GET",
            "COMMAND GETKEYSANDFLAGS LCS a b",
            "COMMAND INFO nosuch",
            "COMMAND DOCS nosuch",
            "COMMAND LIST FILTERBY FOO x",
            "COMMAND LIST FILTERBY",
        ],
    ),
    // ---- lists ----
    (
        (2, 0),
        &[
            "RPUSH l a b c",
            "LPUSH l z y",
            "LRANGE l 0 -1",
            "LRANGE l 1 2",
            "LRANGE l -2 100",
            "LRANGE l 3 1",
            "LRANGE l 10 20",
            "LRANGE nokey 0 -1",
            "LRANGE l x 1",
            "LLEN l",
            "LLEN nokey",
            "LPOP l",
            "RPOP l",
            "LINDEX l 0",
            "LINDEX l -1",
            "LINDEX l 9",
            "LINDEX l x",
            "LINDEX nokey 0",
            "LSET l 0 A",
            "LSET l 9 x",
            "LSET nokey 0 x",
            "LRANGE l 0 -1",
            "LPUSHX nokey a",
            "RPUSHX l d e",
            "LINSERT l BEFORE b B",
            "LINSERT l AFTER e f",
            "LINSERT l AFTER nope x",
            "LINSERT nokey AFTER a x",
            "LINSERT l MIDDLE a x",
            "LRANGE l 0 -1",
            "SET s v",
            "LPUSH s a",
            "LLEN s",
            "LRANGE s 0 1",
            "TYPE l",
            "LPOP nokey",
        ],
    ),
    (
        (6, 2),
        &[
            "RPUSH l a b c d e",
            "LPOP l 2",
            "RPOP l 2",
            "LPOP l 0",
            "RPOP l 5",
            "EXISTS l",
            "LPOP l 2",
            "@7.0 LPOP l -1",
            "@7.0 LPOP l x",
            "LPOP l 1 2",
        ],
    ),
    (
        (6, 0),
        &[
            "RPUSH l a b a c a d",
            "LPOS l a",
            "LPOS l a RANK 2",
            "LPOS l a RANK -1",
            "LPOS l a COUNT 0",
            "LPOS l a COUNT 2 RANK -1",
            "LPOS l a MAXLEN 1 COUNT 0",
            "LPOS l z",
            "LPOS l z COUNT 1",
            "LPOS nokey a",
            "LPOS nokey a COUNT 1",
            "@7.0 LPOS l a RANK 0",
            "LPOS l a COUNT -1",
            "LPOS l a MAXLEN -1",
            "LPOS l a FOO",
            "LREM l -2 a",
            "LRANGE l 0 -1",
            "LREM l 0 z",
            "LREM nokey 0 z",
            "LREM l x a",
            "LTRIM l 1 -2",
            "LRANGE l 0 -1",
            "LTRIM nokey 0 1",
            "LTRIM l 5 1",
            "EXISTS l",
        ],
    ),
    (
        (6, 2),
        &[
            "RPUSH src a b c",
            "LMOVE src dst LEFT RIGHT",
            "LMOVE src dst RIGHT LEFT",
            "LRANGE dst 0 -1",
            "RPOPLPUSH src dst",
            "EXISTS src",
            "RPOPLPUSH src dst",
            "LMOVE dst dst LEFT RIGHT",
            "LRANGE dst 0 -1",
            "LMOVE dst dst UP RIGHT",
            "SET s v",
            "LMOVE dst s LEFT RIGHT",
            "LMOVE s dst LEFT RIGHT",
            "LRANGE dst 0 -1",
        ],
    ),
    (
        (7, 0),
        &[
            "RPUSH dst c a b",
            "LMPOP 2 nokey dst LEFT",
            "LMPOP 1 dst RIGHT COUNT 10",
            "LMPOP 1 dst RIGHT",
            "LMPOP 0 dst RIGHT",
            "LMPOP 2 dst RIGHT",
            "LMPOP 1 dst UP",
            "LMPOP 1 dst LEFT COUNT 0",
            "LMPOP 1 dst LEFT COUNT 1 COUNT 1",
            "SET s v",
            "LMPOP 1 s LEFT",
            "RPUSH b x y",
            "BLMPOP 0 2 a b RIGHT COUNT 5",
            "BLMPOP 0 0 a LEFT",
            "BLMPOP 0.01 1 a LEFT",
        ],
    ),
    (
        (6, 2),
        &[
            "RPUSH b x y",
            "BLPOP a b 0",
            "BRPOP a b 0",
            "RPUSH b x",
            "BLMOVE b c LEFT LEFT 0",
            "BRPOPLPUSH c d 0",
            "BLPOP a x",
            "BLPOP a -1",
            "BLPOP a 1e300",
            "BLPOP a 0.01",
            "BRPOPLPUSH a b 0.01",
            "SET s v",
            "BLPOP s 0",
            "BLMOVE s x LEFT LEFT 0",
            "BLMOVE b c UP LEFT 0",
        ],
    ),
    // Waiters are served FIFO when data arrives, one element each.
    (
        (2, 0),
        &[
            "&1 BLPOP k 0",
            "&2 BRPOP other k 0",
            "&3 BLPOP k 0",
            "RPUSH k 1 2",
            "<1",
            "<2",
            "EXISTS k",
            "LPUSH k 3",
            "<3",
            "EXISTS k",
        ],
    ),
    (
        (6, 2),
        &[
            "&1 BLMOVE src mid LEFT RIGHT 0",
            "&2 BLPOP mid 0",
            "RPUSH src v",
            "<1",
            "<2",
            "EXISTS src mid",
            "&3 BLPOP k 0",
            "SET k s",
            "DEL k",
            "RPUSH tmp x y",
            "RENAME tmp k",
            "<3",
            "LRANGE k 0 -1",
        ],
    ),
    (
        (7, 0),
        &[
            "&1 BLMPOP 0 2 a b RIGHT COUNT 3",
            "SELECT 1",
            "RPUSH b x y z w",
            "MOVE b 0",
            "<1",
            "SELECT 0",
            "LRANGE b 0 -1",
            "!=2 HELLO 3",
            "&2 BLPOP q 0.05",
            "<2",
            "&2 BLPOP q 0",
            "RPUSH q v",
            "<2",
        ],
    ),
    // ---- sets ----
    (
        (7, 2),
        &[
            "SADD s 3 1 2 1",
            "SMEMBERS s",
            "SADD s -5",
            "SMEMBERS s",
            "SADD s x 0",
            "SMEMBERS s",
            "SREM s 2 nope x",
            "SMEMBERS s",
            "SCARD s",
            "SCARD nokey",
            "SISMEMBER s 3",
            "SISMEMBER s 2",
            "SISMEMBER nokey 2",
            "SMEMBERS nokey",
            "SREM s -5 1 3 0",
            "EXISTS s",
            "SREM s a",
            "SET str v",
            "SADD str a",
            "SMEMBERS str",
            "SADD w c b a d",
            "SMEMBERS w",
            "TYPE w",
            "SADD w b e",
            "SREM w b",
            "SMEMBERS w",
        ],
    ),
    (
        (6, 2),
        &[
            "SADD s 1 2 0",
            "SMISMEMBER s 1 3 0",
            "SMISMEMBER nokey 1",
            "SET str v",
            "SMISMEMBER str a",
        ],
    ),
    (
        (7, 2),
        &[
            "SADD s 1 2",
            "OBJECT ENCODING s",
            "SADD s x",
            "OBJECT ENCODING s",
            "SADD t 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16",
            "OBJECT ENCODING t",
            "SADD h aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "OBJECT ENCODING h",
            "SET n 12",
            "OBJECT ENCODING n",
            "SET e hello",
            "OBJECT ENCODING e",
            "RPUSH l a b",
            "OBJECT ENCODING l",
            "HSET hh a 1",
            "OBJECT ENCODING hh",
            "OBJECT ENCODING nokey",
            "OBJECT REFCOUNT nokey",
            "OBJECT FREQ l",
            "OBJECT FOO l",
        ],
    ),
    (
        (2, 0),
        &[
            "SADD a 1 2",
            "SMOVE a b 1",
            "SMOVE a b 9",
            "SMOVE nokey b 1",
            "SMOVE a a 2",
            "SMOVE a a 9",
            "SMOVE a b 2",
            "EXISTS a",
            "SMEMBERS b",
            "SET str v",
            "SMOVE b str 1",
            "SMOVE str b 1",
        ],
    ),
    (
        (7, 2),
        &[
            "SADD a c b a d",
            "SADD b d x c",
            "SADD n 3 1 2",
            "SADD m 2 9 x",
            "SINTER a b",
            "SINTER a nokey",
            "SINTER n m",
            "SUNION a b",
            "SUNION n nokey",
            "SUNION n m",
            "SUNION m n",
            "SDIFF a b",
            "SDIFF a a",
            "SDIFF nokey a",
            "SDIFF m n",
            "SDIFF n m",
            "SINTERSTORE dst a b",
            "SMEMBERS dst",
            "SUNIONSTORE dst n a",
            "SMEMBERS dst",
            "SINTERSTORE dst n m",
            "SMEMBERS dst",
            "SDIFFSTORE dst a a",
            "EXISTS dst",
            "SET str v",
            "SUNION a str",
            "SINTER nokey str",
        ],
    ),
    (
        (7, 0),
        &[
            "SADD a c b a d",
            "SADD b d x c",
            "SINTERCARD 2 a b",
            "SINTERCARD 2 a b LIMIT 1",
            "SINTERCARD 1 nokey",
            "SINTERCARD 0 a",
            "SINTERCARD 3 a b",
            "SINTERCARD 1 a LIMIT -1",
            "SINTERCARD 1 a FOO",
        ],
    ),
    (
        (6, 2),
        &[
            "SADD s a b c",
            "SPOP s 0",
            "SPOP s 1 2",
            "SPOP s -1",
            "SRANDMEMBER s 0",
            "SRANDMEMBER s 5",
            "SRANDMEMBER s x",
            "SRANDMEMBER s 1 2",
            "SPOP s 10",
            "EXISTS s",
            "SPOP s",
            "SPOP s 3",
            "SRANDMEMBER s",
            "SRANDMEMBER s 3",
            "SADD s a b c",
            "SSCAN s 0 COUNT 1",
            "SSCAN s 0 MATCH b",
            "SSCAN nokey 0",
            "!HELLO 3",
            "SMEMBERS s",
            "SPOP s 0",
            "SINTER s nokey",
            "!HELLO 2",
        ],
    ),
    // ---- sorted sets ----
    (
        (6, 2),
        &[
            "ZADD z 1 a 2 b 3 c",
            "ZADD z 1.5 a 10 d",
            "ZRANGE z 0 -1",
            "ZRANGE z 0 -1 WITHSCORES",
            "ZADD z NX 100 a 5 e",
            "ZADD z XX 7 a 8 nope",
            "ZADD z XX CH 7 a 9 b",
            "ZADD z GT 1 a 20 b",
            "ZADD z LT CH 1 a 30 b",
            "ZADD z INCR 2.5 c",
            "ZADD z NX INCR 1 c",
            "ZADD z XX INCR 1 nope",
            "ZADD z NX XX 1 a",
            "ZADD z GT LT 1 a",
            "ZADD z NX GT 1 a",
            "ZADD z INCR 1 a 2 b",
            "ZADD z 1",
            "ZADD z x a",
            "ZADD z nan a",
            "ZADD z inf a -inf b",
            "ZADD z INCR -inf a",
            "ZRANGE z 0 -1 WITHSCORES",
            "ZINCRBY z 0.1 c",
            "ZINCRBY z 1 new",
            "ZINCRBY z x c",
            "ZCARD z",
            "ZCARD nokey",
            "ZSCORE z c",
            "ZSCORE z nope",
            "ZSCORE nokey a",
            "ZMSCORE z c nope a",
            "ZMSCORE nokey a",
            "ZREM z a nope",
            "ZREM nokey a",
            "TYPE z",
            "SET str v",
            "ZADD str 1 a",
            "ZSCORE str a",
            "ZRANGE str 0 1",
        ],
    ),
    (
        (6, 2),
        &[
            "ZADD z 0.1 a 0.2 b 0.30000000000000004 c 1e21 d 1e-7 e -0 f 3.0 g 1e23 h",
            "ZRANGE z 0 -1 WITHSCORES",
            "ZSCORE z h",
            "ZINCRBY z 0.2 a",
            "ZADD k 1 x",
            "ZINCRBY k 1e308 x",
            "ZINCRBY k 1e308 x",
            "ZADD k 1 y",
            "ZINCRBY k inf y",
            "ZINCRBY k -inf y",
        ],
    ),
    (
        (7, 2),
        &[
            "ZADD z 1 a 2 b 3 c 4 d",
            "ZRANK z c",
            "ZRANK z nope",
            "ZREVRANK z c",
            "ZRANK z c WITHSCORE",
            "ZREVRANK z a WITHSCORE",
            "ZRANK z nope WITHSCORE",
            "ZRANK nokey a WITHSCORE",
            "ZRANK z a FOO",
            "ZRANK z a WITHSCORE x",
            "OBJECT ENCODING z",
        ],
    ),
    (
        (6, 2),
        &[
            "ZADD z 1 a 2 b 3 c 4 d 5 e",
            "ZRANGE z 1 2",
            "ZRANGE z -2 -1 WITHSCORES",
            "ZRANGE z 3 1",
            "ZRANGE z 0 -1 REV",
            "ZRANGE z (1 4 BYSCORE",
            "ZRANGE z -inf +inf BYSCORE LIMIT 1 2",
            "ZRANGE z -inf +inf BYSCORE LIMIT 1 -1 WITHSCORES",
            "ZRANGE z -inf +inf BYSCORE LIMIT -1 2",
            "ZRANGE z -inf +inf BYSCORE LIMIT 10 2",
            "ZRANGE z 4 (2 BYSCORE REV",
            "ZRANGE z (4 2 BYSCORE REV LIMIT 0 1",
            "ZRANGE z 0 1 LIMIT 0 1",
            "ZRANGE z x 1",
            "ZRANGE z a b BYSCORE",
            "ZRANGE z [a [c BYLEX",
            "ZRANGE z - + BYLEX LIMIT 1 2",
            "ZRANGE z + - BYLEX REV",
            "ZRANGE z [a [c BYLEX WITHSCORES",
            "ZRANGE z a c BYLEX",
            "ZRANGE z 0 1 FOO",
            "ZRANGE nokey 0 -1",
            "ZREVRANGE z 0 1 WITHSCORES",
            "ZREVRANGE z 0 1 LIMIT 0 1",
            "ZRANGEBYSCORE z 2 4",
            "ZRANGEBYSCORE z (2 (4 WITHSCORES",
            "ZRANGEBYSCORE z -inf +inf LIMIT 2 2",
            "ZRANGEBYSCORE z 1 2 REV",
            "ZREVRANGEBYSCORE z 4 2",
            "ZREVRANGEBYSCORE z +inf -inf WITHSCORES LIMIT 0 2",
            "ZRANGEBYSCORE z 1 x",
            "ZCOUNT z 2 4",
            "ZCOUNT z (2 +inf",
            "ZCOUNT z x 1",
            "ZCOUNT nokey 0 1",
            "ZADD l 0 a 0 b 0 c 0 d 0 e",
            "ZRANGEBYLEX l - +",
            "ZRANGEBYLEX l [b (d",
            "ZRANGEBYLEX l (b + LIMIT 1 1",
            "ZREVRANGEBYLEX l + [c",
            "ZREVRANGEBYLEX l (d - LIMIT 0 2",
            "ZRANGEBYLEX l b d",
            "ZLEXCOUNT l [b [d",
            "ZLEXCOUNT l - +",
            "ZLEXCOUNT l x y",
            "ZRANGESTORE dst z 1 3",
            "ZRANGE dst 0 -1 WITHSCORES",
            "ZRANGESTORE dst z 2 +inf BYSCORE LIMIT 0 2",
            "ZRANGE dst 0 -1 WITHSCORES",
            "ZRANGESTORE dst l [b [c BYLEX",
            "ZRANGE dst 0 -1",
            "ZRANGESTORE dst z 5 1",
            "EXISTS dst",
            "ZRANGESTORE dst nokey 0 1",
            "ZRANGESTORE dst z 0 1 WITHSCORES",
        ],
    ),
    (
        (6, 2),
        &[
            "ZADD z 1 a 2 b 3 c 4 d 5 e",
            "ZREMRANGEBYRANK z 0 1",
            "ZREMRANGEBYRANK z 5 10",
            "ZREMRANGEBYSCORE z (3 4",
            "ZREMRANGEBYSCORE z x 4",
            "ZRANGE z 0 -1",
            "ZADD l 0 a 0 b 0 c",
            "ZREMRANGEBYLEX l [a (c",
            "ZREMRANGEBYLEX l a c",
            "ZRANGE l 0 -1",
            "ZREMRANGEBYRANK l 0 -1",
            "EXISTS l",
            "ZREMRANGEBYRANK nokey 0 1",
        ],
    ),
    (
        (6, 2),
        &[
            "ZADD z 1 a 2 b 3 c 4 d 5 e",
            "ZPOPMIN z",
            "ZPOPMAX z",
            "ZPOPMIN z 2",
            "ZPOPMIN z 0",
            "ZPOPMAX z 10",
            "EXISTS z",
            "ZPOPMIN z",
            "ZPOPMIN z 3",
            "ZPOPMIN z -1",
            "ZPOPMIN z 1 2",
            "ZADD z 1 a 2 b",
            "BZPOPMIN nokey z 0",
            "BZPOPMAX z 0",
            "BZPOPMIN nokey 0.01",
            "BZPOPMIN nokey x",
            "SET str v",
            "ZPOPMIN str",
            "BZPOPMIN str 0",
            "ZADD z 1 a 2 b 3 c",
            "!HELLO 3",
            "ZPOPMIN z",
            "ZPOPMIN z 1",
            "ZRANGE z 0 -1 WITHSCORES",
            "ZSCORE z c",
            "ZMSCORE z c nope",
            "ZINCRBY z 1 c",
            "BZPOPMAX z 0",
            "BZPOPMIN nokey 0.01",
            "!HELLO 2",
        ],
    ),
    (
        (7, 0),
        &[
            "ZADD z 1 a 2 b 3 c",
            "ZMPOP 2 nokey z MIN",
            "ZMPOP 1 z MAX COUNT 5",
            "ZMPOP 1 z MAX",
            "ZMPOP 0 z MIN",
            "ZMPOP 2 z MIN",
            "ZMPOP 1 z UP",
            "ZMPOP 1 z MIN COUNT 0",
            "ZADD z 1 a 2 b",
            "BZMPOP 0 1 z MIN COUNT 1",
            "BZMPOP 0.01 1 nokey MIN",
            "!HELLO 3",
            "BZMPOP 0 1 z MIN COUNT 1",
            "BZMPOP 0.01 1 nokey MIN",
            "!HELLO 2",
        ],
    ),
    (
        (6, 2),
        &[
            "ZADD a 1 x 2 y 3 z",
            "ZADD b 10 y 20 z 30 w",
            "SADD s x w",
            "ZUNION 2 a b",
            "ZUNION 2 a b WITHSCORES",
            "ZUNION 2 a b WEIGHTS 2 3 WITHSCORES",
            "ZUNION 2 a b AGGREGATE MIN WITHSCORES",
            "ZUNION 2 a b AGGREGATE MAX WITHSCORES",
            "ZUNION 3 a b s WITHSCORES",
            "ZUNION 2 a nokey WITHSCORES",
            "ZINTER 2 a b WITHSCORES",
            "ZINTER 2 a s WITHSCORES",
            "ZINTER 2 a nokey",
            "ZDIFF 2 a b WITHSCORES",
            "ZDIFF 2 b s",
            "ZDIFF 1 nokey",
            "ZUNIONSTORE dst 2 a b WEIGHTS 1 -1",
            "ZRANGE dst 0 -1 WITHSCORES",
            "ZINTERSTORE dst 2 a b AGGREGATE MAX",
            "ZRANGE dst 0 -1 WITHSCORES",
            "ZDIFFSTORE dst 2 a a",
            "EXISTS dst",
            "ZUNION 0 a",
            "ZUNIONSTORE dst 0 a",
            "ZINTER 3 a b",
            "ZUNION x a",
            "ZUNION 2 a b WEIGHTS 1",
            "ZUNION 2 a b WEIGHTS 1 x",
            "ZUNION 2 a b AGGREGATE AVG",
            "ZUNIONSTORE dst 2 a b WITHSCORES",
            "ZDIFF 2 a b WEIGHTS 1 2",
            "SET str v",
            "ZUNION 2 a str",
            "ZADD inf1 inf x",
            "ZADD inf2 -inf x",
            "ZUNION 2 inf1 inf2 WITHSCORES",
            "ZINTER 2 inf1 inf2 WITHSCORES",
            "!HELLO 3",
            "ZUNION 2 a b WITHSCORES",
            "!HELLO 2",
        ],
    ),
    (
        (7, 0),
        &[
            "ZADD a 1 x 2 y 3 z",
            "ZADD b 10 y 20 z 30 w",
            "ZINTERCARD 2 a b",
            "ZINTERCARD 2 a b LIMIT 1",
            "ZINTERCARD 1 nokey",
            "ZINTERCARD 0 a",
            "ZINTERCARD 3 a b",
            "ZINTERCARD 1 a LIMIT -1",
            "ZINTERCARD 1 a WITHSCORES",
        ],
    ),
    (
        (6, 2),
        &[
            "ZADD z 1 a 2 b 3 c",
            "ZRANDMEMBER z 0",
            "ZRANDMEMBER z 5",
            "ZRANDMEMBER z 5 WITHSCORES",
            "ZRANDMEMBER z x",
            "ZRANDMEMBER z 1 FOO",
            "ZRANDMEMBER nokey",
            "ZRANDMEMBER nokey 5",
            "ZRANDMEMBER z -9223372036854775807 WITHSCORES",
            "ZSCAN z 0",
            "ZSCAN z 0 MATCH b",
            "ZSCAN nokey 0",
            "ZADD f 0.1 a 1e21 b",
            "ZSCAN f 0",
        ],
    ),
    (
        (7, 2),
        &[
            "ZADD z 1 a",
            "OBJECT ENCODING z",
            "ZADD big 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "OBJECT ENCODING big",
            "ZUNIONSTORE small 1 big",
            "OBJECT ENCODING small",
            "ZADD dup 1 x",
            "ZUNIONSTORE dup2 1 dup",
            "OBJECT ENCODING dup2",
        ],
    ),
    // Blocked BZPOPMIN waiters are served FIFO by ZADD.
    (
        (5, 0),
        &[
            "&1 BZPOPMIN z 0",
            "&2 BZPOPMAX other z 0",
            "ZADD z 1 a 2 b 3 c",
            "<1",
            "<2",
            "ZRANGE z 0 -1",
            "&3 BZPOPMIN q 0",
            "RPUSH q notazset",
            "DEL q",
            "ZADD q 5 v",
            "<3",
        ],
    ),
    // ---- transactions ----
    (
        (2, 0),
        &[
            "MULTI",
            "SET k 1",
            "INCR k",
            "LPUSH k x",
            "GET k",
            "EXEC",
            "EXEC",
            "DISCARD",
            "MULTI",
            "MULTI",
            "SET k 5",
            "DISCARD",
            "GET k",
            "MULTI",
            "EXEC",
        ],
    ),
    (
        (6, 2),
        &[
            "MULTI",
            "WATCH k",
            "SET k 1",
            "GET",
            "EXEC",
            "GET k",
            "MULTI",
            "NOPE",
            "EXEC",
            "MULTI",
            "EXEC x",
            "EXEC",
            "MULTI",
            "BLPOP q 0",
            "BRPOPLPUSH q r 0",
            "BZPOPMIN z 0",
            "EXEC",
            "!HELLO 3",
            "MULTI",
            "BLPOP q 0",
            "SET k v",
            "EXEC",
            "!HELLO 2",
        ],
    ),
    (
        (7, 0),
        &[
            "MULTI",
            "BLMPOP 0 1 q LEFT",
            "BZMPOP 0 1 z MIN",
            "EXEC",
            "MULTI",
            "SAVE",
            "EXEC",
            "BGSAVE foo",
            "!LASTSAVE",
            "!TIME",
        ],
    ),
    (
        (2, 2),
        &[
            "SET k 1",
            "WATCH k",
            "=2 SET k 2",
            "MULTI",
            "SET k 3",
            "EXEC",
            "GET k",
            "MULTI",
            "SET k 3",
            "EXEC",
            "WATCH k",
            "=2 SET j 1",
            "=2 GET k",
            "=2 LPUSH k x",
            "MULTI",
            "GET k",
            "EXEC",
            "WATCH k",
            "UNWATCH",
            "=2 SET k 9",
            "MULTI",
            "EXEC",
            "WATCH k nokey",
            "=2 SET nokey 1",
            "MULTI",
            "EXEC",
            "WATCH k",
            "MULTI",
            "DISCARD",
            "=2 SET k 10",
            "MULTI",
            "GET k",
            "EXEC",
        ],
    ),
    (
        (6, 0),
        &[
            "SET k 1",
            "WATCH k",
            "=2 FLUSHALL",
            "MULTI",
            "EXEC",
            "WATCH k",
            "=2 FLUSHALL",
            "MULTI",
            "EXEC",
            "SET k 1",
            "WATCH k",
            "=2 EXPIRE k 100",
            "MULTI",
            "EXEC",
            "SET a 1",
            "WATCH a",
            "=2 RENAME a b",
            "MULTI",
            "EXEC",
            "SET a 1",
            "WATCH a",
            "=2 MOVE a 1",
            "MULTI",
            "EXEC",
            "=2 SELECT 1",
            "SELECT 1",
            "WATCH a",
            "=2 MOVE a 0",
            "MULTI",
            "EXEC",
            "SELECT 0",
            "WATCH a",
            "=2 SWAPDB 0 1",
            "MULTI",
            "EXEC",
        ],
    ),
    // A push inside a transaction serves waiters after EXEC.
    ((2, 0), &["&1 BLPOP q 0", "MULTI", "RPUSH q a", "RPUSH q b", "EXEC", "<1", "LRANGE q 0 -1"]),
    // ---- pub/sub ----
    (
        (2, 0),
        &[
            "&1 SUBSCRIBE a b",
            "<1",
            "<1",
            "&1 PSUBSCRIBE a*",
            "<1",
            "PUBLISH a hi",
            "<1",
            "<1",
            "PUBLISH zzz hi",
            "PUBLISH b there",
            "<1",
            "&1 PING",
            "<1",
            "&1 PING x",
            "<1",
            "~PUBSUB CHANNELS",
            "PUBSUB CHANNELS b*",
            "PUBSUB NUMSUB a nope",
            "PUBSUB NUMPAT",
            "&1 UNSUBSCRIBE a",
            "<1",
            "&1 PUNSUBSCRIBE",
            "<1",
            "&1 PUNSUBSCRIBE",
            "<1",
            "&1 UNSUBSCRIBE",
            "<1",
            "&1 UNSUBSCRIBE",
            "<1",
            "=1 GET k",
            "PUBLISH a nobody",
        ],
    ),
    (
        (6, 2),
        &[
            "&1 SUBSCRIBE a",
            "<1",
            "&1 GET k",
            "<1",
            "&1 RESET",
            "<1",
            "PUBLISH a x",
            "=1 GET k",
            "MULTI",
            "SUBSCRIBE c",
            "EXEC",
            "PUBLISH c x",
            "RESET",
        ],
    ),
    (
        (7, 0),
        &[
            "&1 SSUBSCRIBE s t",
            "<1",
            "<1",
            "SPUBLISH s m",
            "<1",
            "~PUBSUB SHARDCHANNELS",
            "PUBSUB SHARDNUMSUB s nope",
            "PUBLISH s notshard",
            "&1 SUNSUBSCRIBE s",
            "<1",
            "&1 SUNSUBSCRIBE",
            "<1",
            "MULTI",
            "SSUBSCRIBE s",
            "EXEC",
            "PUBSUB CHANNELS x y",
            "PUBSUB FOO",
        ],
    ),
    (
        (6, 0),
        &[
            "!=2 HELLO 3",
            "&2 SUBSCRIBE c",
            "<2",
            "=2 SET k v",
            "=2 GET k",
            "PUBLISH c m",
            "<2",
            "&2 PSUBSCRIBE c*",
            "<2",
            "PUBLISH c m2",
            "<2",
            "<2",
            "=2 PING",
            "&2 UNSUBSCRIBE",
            "<2",
        ],
    ),
    // ---- CONFIG ----
    (
        (7, 0),
        &[
            "CONFIG GET maxmemory",
            "CONFIG GET maxmemory-policy",
            "CONFIG GET appendfsync",
            "CONFIG GET timeout",
            "CONFIG GET databases",
            "CONFIG GET proto-max-bulk-len",
            "CONFIG GET list-max-listpack-size",
            "CONFIG GET hash-max-ziplist-entries",
            "CONFIG GET nosuchparam",
            "CONFIG GET maxmemory nosuchparam maxmemory",
            "~CONFIG GET maxmemory-clients maxmemory-samples",
            "~CONFIG GET maxmemory-p*",
            "CONFIG SET maxmemory 100mb",
            "CONFIG GET maxmemory",
            "CONFIG SET maxmemory 1gb",
            "CONFIG GET maxmemory",
            "CONFIG SET maxmemory 0",
            "CONFIG SET maxmemory-policy allkeys-lru",
            "CONFIG GET maxmemory-policy",
            "CONFIG SET maxmemory-policy NOEVICTION",
            "CONFIG GET maxmemory-policy",
            "CONFIG SET maxmemory-policy nosuchpolicy",
            "CONFIG SET appendfsync always",
            "CONFIG GET appendfsync",
            "CONFIG SET appendfsync everysec",
            "CONFIG SET timeout 100 tcp-keepalive 200",
            "CONFIG GET timeout",
            "CONFIG GET tcp-keepalive",
            "CONFIG SET timeout 0 tcp-keepalive 300",
            "CONFIG SET hash-max-ziplist-entries 64",
            "CONFIG GET hash-max-listpack-entries",
            "CONFIG GET hash-max-ziplist-entries",
            "CONFIG SET hash-max-listpack-entries 128",
            "CONFIG GET hash-max-ziplist-entries",
            // Back to the built-in default, so a reference server that
            // outlives this run starts the next one unchanged.
            "CONFIG SET hash-max-listpack-entries 512",
            "CONFIG SET timeout notanumber",
            "CONFIG SET timeout -1",
            "CONFIG SET maxmemory notamemory",
            "CONFIG SET appendonly maybe",
            "CONFIG SET appendonly no",
            "CONFIG SET databases 32",
            "CONFIG SET nosuchparam 1",
            "CONFIG SET timeout 1 timeout 2",
            "CONFIG SET timeout",
            "CONFIG SET timeout 5 tcp-keepalive",
            "CONFIG GET timeout",
            "CONFIG SET notify-keyspace-events KEA",
            "CONFIG GET notify-keyspace-events",
            "CONFIG SET notify-keyspace-events lKz",
            "CONFIG GET notify-keyspace-events",
            "CONFIG SET notify-keyspace-events g$lshzxetdnKE",
            "CONFIG GET notify-keyspace-events",
            "CONFIG SET notify-keyspace-events Q",
            "CONFIG SET notify-keyspace-events ",
            "CONFIG GET notify-keyspace-events",
            "CONFIG SET maxmemory-clients 50%",
            "CONFIG GET maxmemory-clients",
            "CONFIG SET maxmemory-clients 0",
            "CONFIG SET unixsocketperm 700",
            "CONFIG GET unixsocketperm",
            "CONFIG SET unixsocketperm 999",
            "CONFIG SET unixsocketperm 0",
            "CONFIG RESETSTAT",
            "CONFIG FOO",
            "CONFIG",
            "~CONFIG HELP",
        ],
    ),
];

fn parse_version(v: &str) -> (u32, u32) {
    let mut it = v.split('.').map(|p| p.parse().unwrap_or(0));
    (it.next().unwrap_or(0), it.next().unwrap_or(0))
}

struct Reference {
    addr: SocketAddr,
    _child: Option<ChildGuard>,
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn reference() -> Option<Reference> {
    if let Ok(addr) = std::env::var("NOIDA_REDIS_REF") {
        let addr = addr.to_socket_addrs().ok()?.next()?;
        return Some(Reference { addr, _child: None });
    }
    let port = TcpListener::bind("127.0.0.1:0").ok()?.local_addr().ok()?.port();
    let child = Command::new("redis-server")
        .args(["--port", &port.to_string(), "--save", "", "--appendonly", "no"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let guard = ChildGuard(child);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::net::TcpStream::connect(addr).is_err() {
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Some(Reference { addr, _child: Some(guard) })
}

fn server_version(c: &mut RawClient) -> (u32, u32) {
    let Value::Bulk(info) = c.run("INFO server") else { panic!("INFO failed") };
    let info = String::from_utf8(info).unwrap();
    let line =
        info.lines().find_map(|l| l.strip_prefix("redis_version:")).expect("redis_version in INFO");
    parse_version(line.trim())
}

fn unordered(v: Value) -> Value {
    match v {
        Value::Array(mut items) => {
            items.sort_by_key(|i| format!("{i:?}"));
            Value::Array(items)
        }
        other => other,
    }
}

/// Splits a script line on spaces only, so `h\*llo` keeps its backslash.
fn args(line: &str) -> Vec<Vec<u8>> {
    line.split(' ').filter(|s| !s.is_empty()).map(|s| s.as_bytes().to_vec()).collect()
}

#[test]
fn replies_match_real_redis() {
    let Some(reference) = reference() else {
        eprintln!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let mut real = RawClient::connect(reference.addr);
    let noida = common::start_noida_redis();
    let mut ours = RawClient::connect(noida);
    let version = server_version(&mut real);

    let mut failures = Vec::new();
    let (mut ran, mut skipped) = (0, 0);
    let (mut lines_compared, mut lines_skipped) = (0, 0);
    for (min, script) in SCRIPTS {
        if version < *min {
            skipped += 1;
            continue;
        }
        ran += 1;
        for c in [&mut real, &mut ours] {
            c.run("RESET");
            c.run("FLUSHALL");
        }
        let mut side: std::collections::HashMap<char, (RawClient, RawClient)> = Default::default();
        for line in *script {
            let (line_min, line) = match line.strip_prefix('@') {
                Some(rest) => {
                    let (v, rest) = rest.split_once(' ').unwrap();
                    (parse_version(v), rest)
                }
                None => ((0, 0), *line),
            };
            let (ignore, line) = match line.strip_prefix('!') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let (is_set, line) = match line.strip_prefix('~') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let conn = match line.as_bytes() {
                [op @ (b'&' | b'<' | b'='), n, ..] => Some((*op, *n as char)),
                _ => None,
            };
            let line = if conn.is_some() { line[2..].trim_start() } else { line };
            let a = args(line);
            let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
            let (mut want, mut got) = match conn {
                None => (real.cmd(&refs), ours.cmd(&refs)),
                Some((op, n)) => {
                    let (r, o) = side.entry(n).or_insert_with(|| {
                        (RawClient::connect(reference.addr), RawClient::connect(noida))
                    });
                    match op {
                        b'&' => {
                            r.send(&refs);
                            o.send(&refs);
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        b'<' => (r.read().expect("reply"), o.read().expect("reply")),
                        _ => (r.cmd(&refs), o.cmd(&refs)),
                    }
                }
            };
            if is_set {
                want = unordered(want);
                got = unordered(got);
            }
            if ignore {
                continue;
            }
            if version < line_min {
                lines_skipped += 1;
                continue;
            }
            lines_compared += 1;
            if want != got {
                failures.push(format!("{line}\n    redis: {want:?}\n    noida: {got:?}"));
            }
        }
    }
    eprintln!(
        "reference Redis {}.{}: ran {ran} scripts ({lines_compared} replies compared), \
         skipped {skipped} scripts and {lines_skipped} lines needing a newer version",
        version.0, version.1
    );
    assert!(
        failures.is_empty(),
        "{} mismatches with real Redis:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// COMMAND INFO and COMMAND DOCS for every command and subcommand noida
/// implements must match real Redis exactly. This checks the generated
/// command table and the code that formats it.
#[test]
fn command_introspection_matches_real_redis() {
    let Some(reference) = reference() else {
        eprintln!("SKIPPED: no reference Redis (set NOIDA_REDIS_REF or install redis-server)");
        return;
    };
    let mut real = RawClient::connect(reference.addr);
    let mut ours = RawClient::connect(common::start_noida_redis());
    if server_version(&mut real) < (7, 0) {
        eprintln!("SKIPPED: COMMAND INFO/DOCS changed in Redis 7.0; reference is older");
        return;
    }
    let Value::Array(names) = ours.run("COMMAND LIST") else { panic!("COMMAND LIST") };
    let mut failures = Vec::new();
    let mut compared = 0;
    for name in names {
        let Value::Bulk(name) = name else { panic!() };
        let name = String::from_utf8(name).unwrap();
        let has_subs = !name.contains('|') && {
            let Value::Array(sub) = ours.run(&format!("COMMAND LIST FILTERBY PATTERN {name}|*"))
            else {
                panic!()
            };
            !sub.is_empty()
        };
        for sub in ["INFO", "DOCS"] {
            let line = format!("COMMAND {sub} {name}");
            let (want, got) = (real.run(&line), ours.run(&line));
            // A container's subcommands come in hash order on real Redis;
            // they are compared one by one instead.
            let (want, got) = if has_subs {
                (drop_subcommands(want), drop_subcommands(got))
            } else {
                (want, got)
            };
            compared += 1;
            if want != got {
                failures.push(format!("{line}\n    redis: {want:?}\n    noida: {got:?}"));
            }
        }
    }
    eprintln!("compared {compared} COMMAND INFO/DOCS replies");
    assert!(failures.is_empty(), "{} mismatches:\n  {}", failures.len(), failures.join("\n  "));
}

/// Removes the trailing subcommand list from a COMMAND INFO or DOCS reply.
fn drop_subcommands(v: Value) -> Value {
    match v {
        // INFO: [[name, arity, ..., subcommands]]
        Value::Array(mut outer) if matches!(outer.first(), Some(Value::Array(_))) => {
            if let Some(Value::Array(info)) = outer.first_mut() {
                info.pop();
            }
            Value::Array(outer)
        }
        // DOCS: [name, [k, v, ..., "subcommands", {...}]]
        Value::Array(mut outer) if outer.len() == 2 => {
            if let Some(Value::Array(doc)) = outer.get_mut(1)
                && let Some(i) = doc.iter().position(|x| *x == Value::bulk("subcommands"))
            {
                doc.truncate(i);
            }
            Value::Array(outer)
        }
        other => other,
    }
}
