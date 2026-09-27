//! Differential tests: every script runs against a real Postgres and against
//! noida-db, and the results must match — values, column names, column types and
//! SQLSTATE codes.
//!
//! The reference server is `NOIDA_POSTGRES_REF=host:port` if set (CI points
//! this at postgres:16), otherwise a local `initdb`/`postgres` pair started in
//! a temporary directory. With neither, the test prints SKIPPED and passes.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};

/// Scripts to compare. `@16` marks a statement that only matches on
/// Postgres 16 or newer, `!` one that runs on both without comparing.
const SCRIPTS: &[&[&str]] = &[
    // Literals, arithmetic and types.
    &[
        "SELECT 1, -1, 2147483647, 2147483648, 9223372036854775807",
        "SELECT 1 + 2, 7 / 2, -7 / 2, 7 % 3, -7 % 3, 2 * 3",
        "SELECT 1.5, 1.50, 0.1 + 0.2, 1/3.0, 10/4.0, 2::numeric/3",
        "SELECT 1e20::float8, 1e15::float8, 0.00001::float8, 100.0::float8, 0.1::float4",
        "SELECT 'a' || 'b', 'a' || 1, length('hello'), upper('aBc'), lower('AbC')",
        "@16 SELECT true, false, NULL, true AND NULL, false AND NULL, true OR NULL",
        "SELECT pg_typeof(1), pg_typeof(1.5), pg_typeof('a'), pg_typeof(1::int8), pg_typeof(now())",
        "SELECT 1 = 1, 1 <> 2, 'a' < 'b', NULL = NULL, NULL IS NULL, 1 IS DISTINCT FROM NULL",
    ],
    &[
        "SELECT 2147483647 + 1",
        "SELECT 1/0",
        "SELECT 'abc'::int",
        "SELECT 1.5::int, 2.5::int, -1.5::int, 2.5::float8::int, 3.5::float8::int",
        "SELECT 32767::int2 + 1",
        "SELECT 'x'::bool",
        "SELECT 'nope'",
        "SELECT nosuchfunction(1)",
        "SELECT 1 + 'a'",
    ],
    // Strings.
    &[
        "SELECT substr('hello world', 1, 5), substring('hello' from 2 for 3), left('abc', 2), right('abc', 2)",
        "SELECT trim('  x  '), btrim('xxaxx', 'x'), ltrim('xxa', 'x'), rtrim('axx', 'x')",
        "SELECT lpad('7', 3, '0'), rpad('7', 3, '0'), repeat('ab', 3), reverse('abc')",
        "SELECT replace('a-b-c', '-', '+'), split_part('a,b,c', ',', 2), strpos('hello', 'll')",
        "SELECT concat('a', NULL, 1), concat_ws('-', 'a', NULL, 'b'), format('%s/%I/%L', 'a', 'b c', 'd')",
        "SELECT md5('abc'), encode('abc'::bytea, 'hex'), encode('abc'::bytea, 'base64'), decode('616263', 'hex')",
        "SELECT 'Hello' LIKE 'H%', 'Hello' LIKE '_ello', 'Hello' ILIKE 'h%', 'a%b' LIKE 'a\\%b'",
        "SELECT 'abc' ~ '^a', 'abc' ~* 'B', 'abc' !~ 'd', regexp_replace('a1b2', '[0-9]', 'x', 'g')",
        "SELECT initcap('hello world'), ascii('A'), chr(66), to_hex(255)",
        "SELECT 'ab'::char(5) || '|', length('ab'::char(5)), 'abc'::varchar(2)",
        "SELECT 'abc'::varchar(2) || 'x'",
        "SELECT string_to_array('a,b,c', ','), array_to_string(ARRAY['a','b'], '-')",
    ],
    // Dates and times (fixed zone so results are deterministic).
    &[
        "SET TimeZone = 'UTC'",
        "SELECT date '2020-02-29', timestamp '2020-01-01 10:20:30.5', timestamptz '2020-01-01 10:00:00+05:30'",
        "SELECT interval '1 day', interval '-1 days 2 hours', interval '1.5 days', interval 'P1Y2M3DT4H5M6S'",
        "SELECT date '2020-01-01' + 30, date '2020-03-01' - date '2020-01-01', date '2020-01-31' + interval '1 month'",
        "SELECT timestamp '2020-01-02 02:00' - timestamp '2020-01-01', age(timestamp '2021-03-15', timestamp '2020-01-20')",
        "SELECT extract(year from date '2020-05-06'), extract(dow from date '2020-05-06'), extract(epoch from timestamptz '2020-01-01 00:00Z')",
        "SELECT date_trunc('month', timestamp '2020-05-06 07:08:09'), date_trunc('hour', timestamp '2020-05-06 07:08:09')",
        "SELECT to_char(timestamp '2020-03-05 14:07:09', 'YYYY-MM-DD HH24:MI:SS'), to_char(date '2020-03-05', 'FMMonth DD, YYYY')",
        "SELECT make_date(2020,2,29), make_interval(days => 3), justify_hours(interval '36 hours')",
        "SELECT timestamp '2020-06-01 12:00' AT TIME ZONE 'UTC'",
        "SELECT date '2021-02-29'",
        "SELECT extract(hour from date '2020-01-01')",
    ],
    // DDL, DML and constraints.
    &[
        "CREATE TABLE t (id serial PRIMARY KEY, name text NOT NULL, email varchar(64) UNIQUE, n int DEFAULT 7)",
        "INSERT INTO t (name, email) VALUES ('ann', 'a@x'), ('bob', 'b@x')",
        "SELECT id, name, email, n FROM t ORDER BY id",
        "INSERT INTO t (name, email) VALUES ('cid', 'a@x')",
        "INSERT INTO t (email) VALUES ('c@x')",
        "UPDATE t SET n = n + 1 WHERE name = 'ann'",
        "SELECT name, n FROM t ORDER BY name",
        "DELETE FROM t WHERE name = 'bob'",
        "SELECT count(*) FROM t",
        "INSERT INTO t (name) VALUES ('dee') RETURNING id, n",
        "SELECT currval('t_id_seq'), nextval('t_id_seq')",
        "DROP TABLE t",
        "SELECT * FROM t",
    ],
    &[
        "CREATE TABLE p (id int PRIMARY KEY, v int CHECK (v > 0))",
        "CREATE TABLE c (id int PRIMARY KEY, p_id int REFERENCES p(id) ON DELETE CASCADE)",
        "INSERT INTO p VALUES (1, 5), (2, 10)",
        "INSERT INTO p VALUES (3, -1)",
        "INSERT INTO c VALUES (1, 1), (2, 1)",
        "INSERT INTO c VALUES (3, 99)",
        "DELETE FROM p WHERE id = 1",
        "SELECT * FROM c ORDER BY id",
        "ALTER TABLE p ADD COLUMN w int DEFAULT 3",
        "SELECT id, v, w FROM p ORDER BY id",
        "ALTER TABLE p DROP COLUMN w",
        "ALTER TABLE p RENAME COLUMN v TO val",
        "SELECT id, val FROM p ORDER BY id",
        "ALTER TABLE p ALTER COLUMN val TYPE bigint",
        "SELECT pg_typeof(val) FROM p LIMIT 1",
    ],
    // Queries: joins, grouping, ordering, subqueries, CTEs, windows.
    &[
        "CREATE TABLE a (id int, name text)",
        "CREATE TABLE b (id int, a_id int, v numeric)",
        "INSERT INTO a VALUES (1,'ann'),(2,'bob'),(3,'cid')",
        "INSERT INTO b VALUES (1,1,10.5),(2,1,20),(3,2,5.25)",
        "SELECT a.name, b.v FROM a JOIN b ON b.a_id = a.id ORDER BY a.name, b.v",
        "SELECT a.name, b.v FROM a LEFT JOIN b ON b.a_id = a.id ORDER BY a.name, b.v",
        "SELECT a.name, b.v FROM b RIGHT JOIN a ON b.a_id = a.id ORDER BY a.name, b.v",
        "SELECT a.name, count(b.id), sum(b.v), avg(b.v), min(b.v), max(b.v) FROM a LEFT JOIN b ON b.a_id=a.id GROUP BY a.name ORDER BY a.name",
        "SELECT count(*), count(v), sum(v), avg(v) FROM b",
        "SELECT a_id, count(*) FROM b GROUP BY a_id HAVING count(*) > 1 ORDER BY a_id",
        "SELECT name FROM a WHERE id IN (SELECT a_id FROM b) ORDER BY name",
        "SELECT name, (SELECT count(*) FROM b WHERE b.a_id = a.id) AS n FROM a ORDER BY name",
        "SELECT name FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id) ORDER BY name",
        "WITH x AS (SELECT a_id, sum(v) s FROM b GROUP BY a_id) SELECT a.name, x.s FROM a JOIN x ON x.a_id = a.id ORDER BY a.name",
        "SELECT id, name FROM a ORDER BY name DESC LIMIT 2 OFFSET 1",
        "SELECT DISTINCT a_id FROM b ORDER BY a_id",
        "SELECT a_id, v, row_number() OVER (PARTITION BY a_id ORDER BY v) FROM b ORDER BY a_id, v",
        "SELECT a_id, sum(v) OVER (ORDER BY a_id) FROM b ORDER BY a_id",
        "SELECT id FROM a UNION SELECT a_id FROM b ORDER BY 1",
        "SELECT id FROM a EXCEPT SELECT a_id FROM b ORDER BY 1",
        "SELECT id FROM a INTERSECT SELECT a_id FROM b ORDER BY 1",
        "SELECT name, count(*) FROM a GROUP BY 1 ORDER BY 1",
        "SELECT v FROM b ORDER BY v DESC NULLS LAST",
        "SELECT a.name FROM a GROUP BY a.id ORDER BY a.name",
        "SELECT name, id FROM a GROUP BY name",
    ],
    // JSON and arrays.
    &[
        "SELECT '{\"a\": 1, \"b\": [1,2]}'::jsonb, '{\"b\":1,\"a\":2,\"b\":3}'::jsonb",
        "SELECT '{\"a\": {\"b\": 2}}'::jsonb -> 'a' -> 'b', '{\"a\": {\"b\": 2}}'::jsonb ->> 'a'",
        "SELECT '{\"a\": 1}'::jsonb @> '{\"a\": 1}', '{\"a\": 1}'::jsonb ? 'a', jsonb_typeof('[]'::jsonb)",
        "SELECT jsonb_build_object('a', 1, 'b', 'x'), json_build_object('a', 1), jsonb_build_array(1, 'a')",
        "SELECT jsonb_array_length('[1,2,3]'::jsonb), jsonb_set('{\"a\":1}'::jsonb, '{a}', '2'), jsonb_pretty('{\"a\":1}'::jsonb)",
        "SELECT to_jsonb(1), to_jsonb('a'::text), to_json(ARRAY[1,2]), row_to_json(row(1,'a'))",
        "SELECT '\"x\"'::jsonb, '1'::jsonb, 'null'::jsonb, '{bad}'::jsonb",
        "SELECT ARRAY[1,2,3], ARRAY['a','b'], ARRAY[[1,2],[3,4]], '{}'::int[]",
        "SELECT array_length(ARRAY[1,2,3], 1), cardinality(ARRAY[1,2]), array_append(ARRAY[1], 2), array_cat(ARRAY[1], ARRAY[2])",
        "SELECT (ARRAY[1,2,3])[2], (ARRAY[1,2,3])[1:2], 2 = ANY(ARRAY[1,2]), 5 = ALL(ARRAY[5,5])",
        "SELECT unnest(ARRAY[1,2,3])",
        "SELECT * FROM generate_series(1, 5, 2)",
        "SELECT generate_series(1,3) AS g, 'x'",
    ],
    // Transactions.
    &[
        "CREATE TABLE t (id int)",
        "BEGIN",
        "INSERT INTO t VALUES (1)",
        "SAVEPOINT s1",
        "INSERT INTO t VALUES (2)",
        "ROLLBACK TO SAVEPOINT s1",
        "INSERT INTO t VALUES (3)",
        "SELECT * FROM t ORDER BY id",
        "COMMIT",
        "SELECT * FROM t ORDER BY id",
        "BEGIN",
        "INSERT INTO t VALUES (4)",
        "ROLLBACK",
        "SELECT count(*) FROM t",
        "COMMIT",
    ],
    // ON CONFLICT and generated columns.
    &[
        "CREATE TABLE t (id int PRIMARY KEY, n int, s text GENERATED ALWAYS AS (n::text) STORED)",
        "INSERT INTO t (id, n) VALUES (1, 1)",
        "INSERT INTO t (id, n) VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = excluded.n + t.n",
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t (id, n) VALUES (1, 9) ON CONFLICT DO NOTHING",
        "SELECT * FROM t ORDER BY id",
        "INSERT INTO t (id, n) VALUES (2, 2) ON CONFLICT (id) DO UPDATE SET n = 0 RETURNING id, n",
    ],
    // Catalog queries drivers and ORMs send.
    &[
        "CREATE TABLE t (id serial PRIMARY KEY, name varchar(20) NOT NULL, data jsonb, ts timestamptz)",
        "CREATE INDEX t_name_idx ON t (name)",
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
        "SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position",
        "SELECT relname, relkind, relnatts FROM pg_class WHERE relname = 't'",
        "SELECT attname, atttypid::regtype::text, attnotnull, attnum FROM pg_attribute WHERE attrelid = 't'::regclass AND attnum > 0 ORDER BY attnum",
        "SELECT conname, contype FROM pg_constraint WHERE conrelid = 't'::regclass ORDER BY conname",
        "SELECT indexname FROM pg_indexes WHERE tablename = 't' ORDER BY indexname",
        "SELECT nspname FROM pg_namespace WHERE nspname IN ('public','pg_catalog','information_schema') ORDER BY nspname",
        "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 't' AND constraint_type <> 'CHECK' ORDER BY constraint_name",
        "SELECT typname, typtype FROM pg_type WHERE typname IN ('int4','text','jsonb','varchar') ORDER BY typname",
        "SELECT current_schema(), current_database()",
        "SELECT format_type('int4'::regtype, -1), format_type('varchar'::regtype, 24), format_type('numeric'::regtype, 655366)",
        "SELECT 't'::regclass::oid = (SELECT oid FROM pg_class WHERE relname='t')",
        "SELECT has_table_privilege('t', 'SELECT'), pg_table_is_visible('t'::regclass)",
    ],
    // Views and schemas.
    &[
        "CREATE SCHEMA s",
        "CREATE TABLE s.t (id int, v text)",
        "INSERT INTO s.t VALUES (1, 'a'), (2, 'b')",
        "CREATE VIEW v AS SELECT id, v FROM s.t WHERE id > 1",
        "SELECT * FROM v",
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name IN ('t','v') ORDER BY table_schema, table_name",
        "SET search_path = s, public",
        "SELECT count(*) FROM t",
        "RESET search_path",
        "DROP VIEW v",
        "DROP SCHEMA s CASCADE",
        "SELECT count(*) FROM s.t",
    ],
    // Django: schema introspection and sequence reset.
    &[
        "CREATE TABLE dj_author (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name varchar(50) NOT NULL)",
        "CREATE TABLE dj_book (id serial PRIMARY KEY, title varchar(100) NOT NULL DEFAULT 'x', author_id bigint NOT NULL REFERENCES dj_author (id) DEFERRABLE INITIALLY DEFERRED, price numeric(8,2), UNIQUE (title, author_id))",
        "CREATE INDEX dj_book_title_idx ON dj_book (title DESC)",
        "CREATE INDEX dj_book_author_idx ON dj_book (author_id)",
        "SELECT c.relname, CASE WHEN c.relispartition THEN 'p' WHEN c.relkind IN ('m', 'v') THEN 'v' ELSE 't' END FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1",
        "SELECT a.attname AS column_name, NOT (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS is_nullable, pg_get_expr(ad.adbin, ad.adrelid) AS column_default, CASE WHEN collname = 'default' THEN NULL ELSE collname END AS collation, a.attidentity != '' AS is_autofield FROM pg_attribute a LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum LEFT JOIN pg_collation co ON a.attcollation = co.oid JOIN pg_type t ON a.atttypid = t.oid JOIN pg_class c ON a.attrelid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind IN ('f', 'm', 'p', 'r', 'v') AND c.relname = 'dj_book' AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND a.attnum > 0 AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY a.attnum",
        "SELECT c.conname, array(SELECT attname FROM unnest(c.conkey) WITH ORDINALITY cols(colid, arridx) JOIN pg_attribute AS ca ON cols.colid = ca.attnum WHERE ca.attrelid = c.conrelid ORDER BY cols.arridx), c.contype, (SELECT fkc.relname || '.' || fka.attname FROM pg_attribute AS fka JOIN pg_class AS fkc ON fka.attrelid = fkc.oid WHERE fka.attrelid = c.confrelid AND fka.attnum = c.confkey[1]), cl.reloptions FROM pg_constraint AS c JOIN pg_class AS cl ON c.conrelid = cl.oid WHERE cl.relname = 'dj_book' AND pg_catalog.pg_table_is_visible(cl.oid) ORDER BY c.conname",
        "SELECT indexname, array_agg(attname ORDER BY arridx), indisunique, indisprimary, array_agg(ordering ORDER BY arridx), amname, exprdef, s2.attoptions FROM (SELECT c2.relname as indexname, idx.*, attr.attname, am.amname, CASE WHEN idx.indexprs IS NOT NULL THEN pg_get_indexdef(idx.indexrelid) END AS exprdef, CASE am.amname WHEN 'btree' THEN CASE (option & 1) WHEN 1 THEN 'DESC' ELSE 'ASC' END END as ordering, c2.reloptions as attoptions FROM (SELECT *, unnest(i.indkey) as key, unnest(i.indoption) as option, generate_subscripts(i.indkey, 1) as arridx FROM pg_index i) idx LEFT JOIN pg_class c ON idx.indrelid = c.oid LEFT JOIN pg_class c2 ON idx.indexrelid = c2.oid LEFT JOIN pg_am am ON c2.relam = am.oid LEFT JOIN pg_attribute attr ON attr.attrelid = c.oid AND attr.attnum = idx.key WHERE c.relname = 'dj_book' AND pg_catalog.pg_table_is_visible(c.oid)) s2 GROUP BY indexname, indisunique, indisprimary, amname, exprdef, attoptions ORDER BY indexname",
        "SELECT s.relname AS sequence_name, a.attname AS column_name FROM pg_class s JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass JOIN pg_attribute a ON d.refobjid = a.attrelid AND d.refobjsubid = a.attnum JOIN pg_class tbl ON tbl.oid = d.refobjid AND tbl.relname = 'dj_book' AND pg_catalog.pg_table_is_visible(tbl.oid) WHERE s.relkind = 'S'",
        "INSERT INTO dj_author (name) VALUES ('ann'), ('bob')",
        "INSERT INTO dj_book (title, author_id, price) VALUES ('a', 1, 9.99)",
        "SELECT setval(pg_get_serial_sequence('\"dj_book\"','id'), coalesce(max(\"id\"), 1), max(\"id\") IS NOT null) FROM \"dj_book\"",
        "SELECT pg_get_serial_sequence('dj_author', 'id'), pg_get_serial_sequence('dj_book', 'title')",
        "SELECT \"dj_book\".\"id\", \"dj_book\".\"title\" FROM \"dj_book\" INNER JOIN \"dj_author\" ON (\"dj_book\".\"author_id\" = \"dj_author\".\"id\") WHERE \"dj_author\".\"name\" = 'ann' ORDER BY \"dj_book\".\"id\" ASC LIMIT 21",
        "SELECT (1) AS \"a\" FROM \"dj_book\" WHERE \"dj_book\".\"id\" = 1 LIMIT 1",
        "SELECT COUNT(*) AS \"__count\" FROM \"dj_book\"",
        "TRUNCATE \"dj_book\", \"dj_author\" RESTART IDENTITY CASCADE",
        "SELECT count(*) FROM dj_book",
        "ALTER TABLE dj_book ALTER COLUMN price SET DEFAULT 0, ALTER COLUMN title DROP DEFAULT",
        "ALTER TABLE dj_book ADD CONSTRAINT dj_book_price_chk CHECK (price >= 0)",
        "ALTER TABLE dj_book DROP CONSTRAINT dj_book_price_chk",
        "ALTER TABLE dj_book RENAME TO dj_books",
        "SELECT count(*) FROM dj_books",
        "SELECT \"dj_books\".* FROM \"dj_books\" WHERE UPPER(\"dj_books\".\"title\"::text) LIKE UPPER('%a%')",
    ],
    // Prisma: migration bookkeeping and introspection.
    &[
        "!SELECT version()",
        "SELECT current_setting('server_version_num')::integer >= 120000, current_schema()",
        "SELECT pg_advisory_lock(72707369)",
        "SELECT pg_advisory_unlock(72707369)",
        "SELECT set_config('search_path', 'public', false)",
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '_prisma_migrations')",
        "CREATE TABLE \"_prisma_migrations\" (id VARCHAR(36) PRIMARY KEY NOT NULL, checksum VARCHAR(64) NOT NULL, finished_at TIMESTAMPTZ, migration_name VARCHAR(255) NOT NULL, logs TEXT, rolled_back_at TIMESTAMPTZ, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), applied_steps_count INTEGER NOT NULL DEFAULT 0)",
        "CREATE TYPE \"Role\" AS ENUM ('USER', 'ADMIN')",
        "CREATE TABLE \"User\" (\"id\" SERIAL NOT NULL, \"email\" TEXT NOT NULL, \"name\" TEXT, \"role\" \"Role\" NOT NULL DEFAULT 'USER', \"createdAt\" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT \"User_pkey\" PRIMARY KEY (\"id\"))",
        "CREATE UNIQUE INDEX \"User_email_key\" ON \"User\"(\"email\")",
        "CREATE TABLE \"Post\" (\"id\" SERIAL NOT NULL, \"title\" TEXT NOT NULL, \"authorId\" INTEGER NOT NULL, CONSTRAINT \"Post_pkey\" PRIMARY KEY (\"id\"))",
        "ALTER TABLE \"Post\" ADD CONSTRAINT \"Post_authorId_fkey\" FOREIGN KEY (\"authorId\") REFERENCES \"User\"(\"id\") ON DELETE RESTRICT ON UPDATE CASCADE",
        "SELECT namespace.nspname as namespace FROM pg_namespace namespace WHERE namespace.nspname = ANY ( ARRAY['public'] ) ORDER BY 1",
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = ANY ( ARRAY['public'] ) AND table_type = 'BASE TABLE' ORDER BY 1",
        "SELECT table_name, column_name, data_type, udt_name, column_default, is_nullable, is_identity, character_maximum_length, numeric_precision, datetime_precision FROM information_schema.columns WHERE table_schema = 'public' AND table_name IN ('User', 'Post') ORDER BY table_name, ordinal_position",
        "SELECT e.enumlabel, t.typname FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid ORDER BY e.enumsortorder",
        "SELECT tc.constraint_name, tc.table_name, kcu.column_name, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema WHERE tc.table_schema = 'public' ORDER BY 1, 3",
        "SELECT rc.constraint_name, rc.update_rule, rc.delete_rule FROM information_schema.referential_constraints rc WHERE rc.constraint_schema = 'public'",
        "SELECT ix.indexrelid::regclass::text AS index_name, ix.indisunique, ix.indisprimary FROM pg_index ix WHERE ix.indrelid = '\"User\"'::regclass ORDER BY 1",
        "SELECT extname, extversion FROM pg_extension",
        "INSERT INTO \"User\" (\"email\", \"name\") VALUES ('a@x', 'ann') RETURNING \"id\", \"role\"::text",
        "SELECT \"public\".\"User\".\"id\", \"public\".\"User\".\"role\"::text FROM \"public\".\"User\" WHERE (\"public\".\"User\".\"email\" = 'a@x' AND 1=1) OFFSET 0",
        "INSERT INTO \"User\" (\"email\") VALUES ('a@x')",
        "UPDATE \"User\" SET \"role\" = 'ADMIN' WHERE \"id\" = 1 RETURNING \"role\"::text",
        "INSERT INTO \"Post\" (\"title\", \"authorId\") VALUES ('t', 99)",
        "DELETE FROM \"User\" WHERE id = 1",
        "INSERT INTO \"User\" (\"role\") VALUES ('NOPE')",
        "DROP TYPE \"Role\"",
    ],
    // Hibernate: dialect and metadata lookups.
    &[
        "CREATE TABLE hb_customer (id bigint NOT NULL, name varchar(255), PRIMARY KEY (id))",
        "CREATE SEQUENCE hb_customer_seq START WITH 1 INCREMENT BY 50",
        "CREATE TABLE hb_order (id bigint NOT NULL, customer_id bigint, total numeric(19,2), placed timestamp(6), PRIMARY KEY (id))",
        "ALTER TABLE IF EXISTS hb_order ADD CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES hb_customer",
        "SELECT sequence_name, start_value, minimum_value, maximum_value, increment FROM information_schema.sequences WHERE sequence_schema = 'public' ORDER BY 1",
        "SELECT nextval('hb_customer_seq'), nextval('hb_customer_seq')",
        "SELECT relname FROM pg_class WHERE relkind = 'S' ORDER BY 1",
        "SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' ORDER BY 1",
        "SHOW transaction isolation level",
        "SHOW standard_conforming_strings",
        "!SHOW server_version_num",
        "SET TimeZone = 'UTC'",
        "SHOW TimeZone",
        "SELECT current_setting('transaction_isolation'), current_setting('DateStyle'), current_setting('client_encoding')",
        "insert into hb_customer (name, id) values ('c1', 1)",
        "insert into hb_order (customer_id, placed, total, id) values (1, '2020-01-01 10:00:00', 12.50, 1)",
        "select o1_0.id, o1_0.total, c1_0.name from hb_order o1_0 left join hb_customer c1_0 on c1_0.id = o1_0.customer_id where o1_0.id = 1",
        "select count(*) from hb_order o1_0 where o1_0.placed >= '2019-01-01'::timestamp",
        "select c1_0.id, c1_0.name from hb_customer c1_0 order by c1_0.name fetch first 10 rows only",
        "update hb_customer set name = 'c2' where id = 1",
        "delete from hb_order where id = 1",
        "drop table if exists hb_order cascade",
        "drop sequence if exists hb_customer_seq",
        "drop table if exists hb_missing cascade",
    ],
    // More SQL: numeric and aggregate functions, sequences, indexes, truncate.
    &[
        "SELECT abs(-5), ceil(1.2), floor(-1.2), round(2.5), round(1234.5678, 2), trunc(-1.9), sign(-3), mod(10, 3), power(2, 10), sqrt(16)",
        "SELECT greatest(1, 5, 3), least(1, 5, 3), coalesce(NULL, 2), nullif(1, 1), nullif(1, 2)",
        "SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' END, CASE 3 WHEN 1 THEN 'x' ELSE 'y' END",
        "SELECT 5 BETWEEN 1 AND 10, 5 NOT BETWEEN 1 AND 4, 'b' IN ('a','b'), 3 NOT IN (1,2)",
        "SELECT cast('12' AS int) + 1, '12'::numeric(5,1), 1::bool, 0::bool, 'true'::bool",
        "SELECT bool_and(x), bool_or(x), string_agg(y, ',' ORDER BY y), array_agg(y ORDER BY y DESC) FROM (VALUES (true, 'a'), (false, 'b'), (true, 'c')) t(x, y)",
        "SELECT count(DISTINCT x), sum(x), avg(x), min(x), max(x) FROM (VALUES (1), (1), (2), (NULL)) t(x)",
        "SELECT x, rank() OVER (ORDER BY x), dense_rank() OVER (ORDER BY x), lag(x) OVER (ORDER BY x), lead(x) OVER (ORDER BY x) FROM (VALUES (1), (1), (2), (3)) t(x)",
        "CREATE SEQUENCE sq START 10 INCREMENT 5",
        "SELECT nextval('sq'), nextval('sq'), currval('sq'), setval('sq', 100), nextval('sq')",
        "SELECT last_value, is_called FROM sq",
        "ALTER SEQUENCE sq RESTART WITH 1",
        "SELECT nextval('sq')",
        "CREATE TABLE u (id int, v text)",
        "CREATE UNIQUE INDEX u_id ON u (id)",
        "INSERT INTO u VALUES (1, 'a'), (1, 'b')",
        "INSERT INTO u VALUES (2, NULL), (3, NULL)",
        "SELECT v IS NULL, count(*) FROM u GROUP BY 1 ORDER BY 1",
        "UPDATE u SET id = id + 10 RETURNING id",
        "DELETE FROM u WHERE id > 12 RETURNING *",
        "TRUNCATE u",
        "SELECT count(*) FROM u",
        "DROP INDEX u_id",
        "DROP INDEX u_id",
        "DROP TABLE IF EXISTS nothere",
        "CREATE TABLE u (id int)",
        "CREATE TABLE IF NOT EXISTS u (id int)",
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(n, s) ORDER BY n DESC",
        "SELECT 1 AS a, 2 AS a",
        "SELECT x FROM generate_series(1, 3) AS x WHERE x <> 2",
        "SELECT * FROM u WHERE nosuchcol = 1",
        "INSERT INTO u (nosuch) VALUES (1)",
        "SELECT 1 FROM",
        "SELEC 1",
    ],
    // Unique indexes, enums, bitwise operators, ordered aggregates, sequences.
    &[
        "CREATE TABLE ui (id int, email text, deleted bool DEFAULT false)",
        "CREATE UNIQUE INDEX ui_email ON ui (lower(email)) WHERE NOT deleted",
        "INSERT INTO ui VALUES (1, 'A@x', false)",
        "INSERT INTO ui VALUES (2, 'a@X', false)",
        "INSERT INTO ui VALUES (3, 'a@x', true)",
        "UPDATE ui SET deleted = false WHERE id = 3",
        "SELECT id FROM ui ORDER BY id",
        "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
        "CREATE TABLE person (n text, m mood DEFAULT 'ok')",
        "INSERT INTO person VALUES ('a', 'happy'), ('b', DEFAULT)",
        "INSERT INTO person VALUES ('c', 'angry')",
        "!SELECT n, m, m > 'sad' FROM person ORDER BY m",
        "DROP TYPE mood",
        "DROP TYPE mood CASCADE",
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'person' ORDER BY ordinal_position",
        "SELECT 6::int2 & 3, 6 | 1::int2, 1::int2 << 3, pg_typeof(1::int2 & 3), 5 # 1, ~5",
        "SELECT array_agg(v ORDER BY v DESC NULLS FIRST), string_agg(v::text, '-' ORDER BY v) FROM (VALUES (1), (NULL), (3), (2)) t(v)",
        "SELECT g, array_agg(v ORDER BY v DESC) FROM (VALUES (1, 'a'), (1, 'c'), (2, 'b')) t(g, v) GROUP BY g ORDER BY g",
        "CREATE SEQUENCE sq1 AS smallint INCREMENT BY 3 MINVALUE 10 MAXVALUE 20 START 11 CYCLE",
        "SELECT nextval('sq1'), nextval('sq1'), nextval('sq1'), nextval('sq1')",
        "ALTER SEQUENCE sq1 RESTART WITH 12 NO CYCLE",
        "SELECT nextval('sq1'), nextval('sq1')",
        "ALTER SEQUENCE sq1 MAXVALUE 15",
        "SELECT nextval('sq1')",
        "SELECT nextval('sq1')",
        "CREATE SEQUENCE sq2 START 0",
        "CREATE SEQUENCE sq3 AS text",
        "CREATE SEQUENCE sq4 INCREMENT 0",
        "CREATE SEQUENCE sq5 MAXVALUE 40000 AS smallint",
        "ALTER SEQUENCE nosuchseq RESTART",
        "ALTER SEQUENCE IF EXISTS nosuchseq RESTART",
        "CREATE SEQUENCE sq1",
        "CREATE SEQUENCE IF NOT EXISTS sq1",
        "ALTER SEQUENCE sq1 RENAME TO sq1b",
        "SELECT nextval('sq1')",
        "SELECT sequencename, data_type, start_value, min_value, max_value, increment_by, cycle FROM pg_sequences ORDER BY 1",
        "DROP SEQUENCE sq1b",
        "DROP SEQUENCE sq1b",
        "DROP SEQUENCE IF EXISTS sq1b",
    ],
    // What psql's \d and other tools rely on: schema-qualified serials, joins
    // after a comma in FROM, relhastriggers, qualified reg* output.
    &[
        "CREATE SCHEMA app",
        "CREATE TYPE app.mood AS ENUM ('sad', 'ok')",
        "CREATE TABLE app.authors (id serial PRIMARY KEY, name varchar(40) NOT NULL UNIQUE)",
        "CREATE TABLE app.books (id bigserial PRIMARY KEY, author_id int REFERENCES app.authors (id) ON DELETE CASCADE, m app.mood)",
        "INSERT INTO app.authors (name) VALUES ('ann')",
        "INSERT INTO app.books (author_id, m) VALUES (1, 'ok')",
        "SELECT column_default FROM information_schema.columns WHERE table_schema = 'app' AND column_default IS NOT NULL ORDER BY table_name",
        "SELECT relname, relhastriggers FROM pg_class WHERE relnamespace = 'app'::regnamespace AND relkind = 'r' ORDER BY 1",
        "SELECT 'app.authors'::regclass::text, 'app.mood'::regtype::text, format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'app.books'::regclass AND attname = 'm'",
        "SELECT conname, pg_get_constraintdef(oid, true) FROM pg_constraint WHERE conrelid = 'app.books'::regclass AND contype = 'f'",
        "SET search_path = app",
        "SELECT 'authors'::regclass::text, 'mood'::regtype::text, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'books'::regclass AND contype = 'f'",
        "RESET search_path",
        "SELECT i.indexrelid::regclass::text, con.contype FROM pg_class c, pg_index i LEFT JOIN pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid = 'app.authors'::regclass AND c.oid = i.indrelid ORDER BY 1",
        "CREATE TABLE ja (id int)",
        "CREATE TABLE jb (id int, v text)",
        "CREATE TABLE jc (id int, w text)",
        "INSERT INTO ja VALUES (1), (2)",
        "INSERT INTO jb VALUES (1, 'b1'), (2, 'b2')",
        "INSERT INTO jc VALUES (1, 'c1')",
        "SELECT ja.id, jb.v, jc.w FROM ja, jb LEFT JOIN jc ON (jc.id = jb.id) ORDER BY 1, 2",
        "SELECT ja.id, jb.v, jc.w FROM ja, jb JOIN jc USING (id) ORDER BY 1, 2",
        "SELECT ja.id, jb.v, jc.w FROM ja CROSS JOIN jb, jc WHERE jc.id = jb.id ORDER BY 1, 2",
        "SELECT getdatabaseencoding()",
    ],
    // Dropped columns, escape strings, GROUP BY on a primary key, LATERAL,
    // multi-array unnest.
    &[
        "CREATE TABLE dc (id serial PRIMARY KEY, name text, a text, b text)",
        "INSERT INTO dc (a, b) VALUES ('p', 'q')",
        "ALTER TABLE dc DROP COLUMN name",
        "SELECT * FROM dc",
        "INSERT INTO dc (a, b) SELECT * FROM (VALUES ('r', 's')) v",
        "UPDATE dc SET b = b || '!' WHERE a = 'r' RETURNING *",
        "DELETE FROM dc WHERE a = 'p' RETURNING *",
        "SELECT * FROM dc x JOIN dc y ON x.a = y.a",
        "SELECT E'a\\\\\"b', E'x\\ny', E'tab\\there', E'q\\'q', E'back\\\\\\\\slash'",
        "CREATE TABLE ga (id int PRIMARY KEY, name text, n int)",
        "CREATE TABLE gb (id int, a_id int)",
        "INSERT INTO ga VALUES (1, 'x', 5), (2, 'y', 6)",
        "INSERT INTO gb VALUES (1, 1), (2, 1), (3, 2)",
        "SELECT ga.name, ga.n, count(gb.id) FROM ga LEFT JOIN gb ON gb.a_id = ga.id GROUP BY ga.id ORDER BY ga.name",
        "SELECT name, count(*) FROM ga GROUP BY n",
        "CREATE TABLE lt (id int PRIMARY KEY, arr int[])",
        "INSERT INTO lt VALUES (1, ARRAY[10, 20]), (2, ARRAY[30]), (3, ARRAY[]::int[])",
        "SELECT id, x FROM lt, unnest(lt.arr) AS x ORDER BY id, x",
        "SELECT id, x, o FROM lt, unnest(lt.arr) WITH ORDINALITY t(x, o) ORDER BY id, o",
        "SELECT id, x FROM lt JOIN LATERAL unnest(lt.arr) AS x ON true ORDER BY id, x",
        "SELECT id, x FROM lt LEFT JOIN LATERAL unnest(lt.arr) AS x ON true ORDER BY id, x",
        "SELECT lt.id, sq.m FROM lt, LATERAL (SELECT max(x) m FROM unnest(lt.arr) x) sq ORDER BY 1",
        "SELECT a.id, b.id, x FROM lt a, lt b, unnest(ARRAY[a.id, b.id]) x WHERE a.id < b.id ORDER BY 1, 2, 3",
        "SELECT unnest(ARRAY[1,2], ARRAY['a','b']), unnest(ARRAY[9], ARRAY['z','w'])",
    ],
];

fn main_test_body() {}

struct Ref {
    client: Client,
    version: u32,
    _server: Option<Server>,
}

struct Server {
    child: Child,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Connects to the reference server named by `NOIDA_POSTGRES_REF`, or starts
/// a local one with initdb.
fn reference() -> Option<Ref> {
    if let Ok(hostport) = std::env::var("NOIDA_POSTGRES_REF") {
        let (host, port) = hostport.split_once(':').unwrap_or((hostport.as_str(), "5432"));
        let user = std::env::var("NOIDA_POSTGRES_REF_USER").unwrap_or_else(|_| "postgres".into());
        let password =
            std::env::var("NOIDA_POSTGRES_REF_PASSWORD").unwrap_or_else(|_| "postgres".into());
        let url =
            format!("host={host} port={port} user={user} password={password} dbname=postgres");
        let client = wait_for(&url)?;
        let version = server_version(&url)?;
        return Some(Ref { client, version, _server: None });
    }
    let bin =
        ["/usr/lib/postgresql/16/bin", "/usr/lib/postgresql/15/bin", "/usr/lib/postgresql/14/bin"]
            .into_iter()
            .map(Path::new)
            .find(|p| p.join("initdb").exists())?;
    let dir = std::env::temp_dir().join(format!("noida-db-pgref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let data = dir.join("data");
    std::fs::create_dir_all(&data).ok()?;
    let ok = Command::new(bin.join("initdb"))
        .args([
            "-D",
            data.to_str()?,
            "-A",
            "trust",
            "-U",
            "postgres",
            "--no-sync",
            "--locale=C",
            "-E",
            "UTF8",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?
        .success();
    if !ok {
        return None;
    }
    let port = free_port();
    let child = Command::new(bin.join("postgres"))
        .args([
            "-D",
            data.to_str()?,
            "-p",
            &port.to_string(),
            // TCP only: a socket path under a long temp dir exceeds the OS limit.
            "-c",
            "unix_socket_directories=",
            "-c",
            "listen_addresses=127.0.0.1",
            "-c",
            "timezone=UTC",
            "-c",
            "fsync=off",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let server = Server { child, dir };
    let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
    let client = wait_for(&url)?;
    let version = server_version(&url)?;
    Some(Ref { client, version, _server: Some(server) })
}

fn wait_for(url: &str) -> Option<Client> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match Client::connect(url, NoTls) {
            Ok(c) => return Some(c),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(_) => return None,
        }
    }
}

fn server_version(url: &str) -> Option<u32> {
    let mut c = Client::connect(url, NoTls).ok()?;
    let row = c.query_one("SHOW server_version_num", &[]).ok()?;
    row.get::<_, &str>(0).parse().ok()
}

/// One statement's observable result.
#[derive(PartialEq, Debug)]
enum Outcome {
    /// Column names, then rows of text values.
    Rows(Vec<String>, Vec<Vec<Option<String>>>),
    Tag(String),
    Error(String),
}

fn run(client: &mut Client, sql: &str) -> Outcome {
    match client.simple_query(sql) {
        Err(e) => match e.as_db_error() {
            Some(db) => Outcome::Error(db.code().code().to_string()),
            None => Outcome::Error(format!("connection: {e}")),
        },
        Ok(messages) => {
            let mut names = vec![];
            let mut rows = vec![];
            let mut tag = None;
            for m in messages {
                match m {
                    SimpleQueryMessage::Row(r) => {
                        if names.is_empty() {
                            names = r.columns().iter().map(|c| c.name().to_string()).collect();
                        }
                        rows.push((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect());
                    }
                    SimpleQueryMessage::RowDescription(cols) => {
                        names = cols.iter().map(|c| c.name().to_string()).collect();
                    }
                    SimpleQueryMessage::CommandComplete(n) => {
                        tag = Some(n.to_string());
                    }
                    _ => {}
                }
            }
            if names.is_empty() {
                Outcome::Tag(tag.map(|t| t.to_string()).unwrap_or_default())
            } else {
                Outcome::Rows(names, rows)
            }
        }
    }
}

/// Column types as the extended protocol reports them (skipped for
/// statements the driver cannot prepare).
fn describe(client: &mut Client, sql: &str) -> Option<Vec<String>> {
    if !sql.trim_start().to_lowercase().starts_with("select") {
        return None;
    }
    let stmt = client.prepare(sql).ok()?;
    Some(stmt.columns().iter().map(|c| c.type_().name().to_string()).collect())
}

#[test]
fn differential() {
    main_test_body();
    let Some(mut reference) = reference() else {
        println!("SKIPPED: no reference Postgres (set NOIDA_POSTGRES_REF or install postgresql)");
        return;
    };
    let addr = noida::postgres::spawn("127.0.0.1:0").expect("start noida-db");
    let noida_url = format!("host=127.0.0.1 port={} user=postgres dbname=postgres", addr.port());

    let mut compared = 0usize;
    let mut skipped = 0usize;
    let mut failures = vec![];
    for (i, script) in SCRIPTS.iter().enumerate() {
        // Each script gets a clean schema on both servers.
        let mut mine = Client::connect(&noida_url, NoTls).expect("connect to noida-db");
        reset(&mut reference.client);
        reset(&mut mine);
        for raw in script.iter() {
            let (sql, compare, min_version) = directives(raw);
            let want = run(&mut reference.client, sql);
            let got = run(&mut mine, sql);
            if !compare || reference.version < min_version {
                skipped += 1;
                continue;
            }
            compared += 1;
            if want != got {
                failures.push(format!(
                    "script {i}: {sql}\n  postgres: {want:?}\n  noida-db:    {got:?}"
                ));
                continue;
            }
            // Result column types must match too.
            if let Some(want_types) = describe(&mut reference.client, sql) {
                let got_types = describe(&mut mine, sql);
                compared += 1;
                if got_types.as_ref() != Some(&want_types) {
                    failures.push(format!(
                        "script {i} column types: {sql}\n  postgres: {want_types:?}\n  noida-db:    {got_types:?}"
                    ));
                }
            }
        }
    }
    println!(
        "compared {compared} results against PostgreSQL {} ({skipped} skipped)",
        reference.version
    );
    if !failures.is_empty() {
        let shown: Vec<String> = failures.iter().take(40).cloned().collect();
        panic!("{} differences:\n{}", failures.len(), shown.join("\n"));
    }
}

fn reset(client: &mut Client) {
    let _ = client.simple_query("ROLLBACK");
    // Drop every schema a script may have created, not just a hardcoded
    // name: the reference server (NOIDA_POSTGRES_REF) is one long-lived
    // process shared by every test binary in a CI job, and by this test's
    // own multiple invocations against a local server, so anything a script
    // leaves behind must not leak into the next one.
    if let Ok(rows) = client.query(
        "SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname NOT IN ('information_schema', 'public')",
        &[],
    ) {
        for row in rows {
            let name: String = row.get(0);
            let _ = client.simple_query(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident(&name)
            ));
        }
    }
    let _ = client.simple_query("DROP SCHEMA public CASCADE; CREATE SCHEMA public; RESET ALL");
}

/// Double-quotes an identifier the way Postgres would need it.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Strips `!` (run but don't compare) and `@NN` (minimum server version).
fn directives(raw: &str) -> (&str, bool, u32) {
    let mut sql = raw;
    let mut compare = true;
    let mut min_version = 0;
    loop {
        if let Some(rest) = sql.strip_prefix('!') {
            compare = false;
            sql = rest.trim_start();
            continue;
        }
        if let Some(rest) = sql.strip_prefix('@') {
            let (v, rest) = rest.split_once(' ').unwrap_or((rest, ""));
            min_version = v.parse::<u32>().unwrap_or(0) * 10_000;
            sql = rest.trim_start();
            continue;
        }
        break;
    }
    (sql, compare, min_version)
}
