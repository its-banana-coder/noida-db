"""Postgres edge cases for failure_diff.py: each scenario runs on noida-db
and a real Postgres, and every step's rows/error must match."""

A = "a"


def steps(*sqls):
    return [(A, s) for s in sqls]


PG_EDGES = {
    "pg edge: numbers and casts": steps(
        "SELECT 7 / 2, -7 / 2, 7 % -3, -7 % 3, 7.0 / 2, 1 / 3::numeric",
        "SELECT 2147483647 + 1",
        "SELECT 2147483647::bigint + 1, (-2147483648)::int, 32767::smallint + 1::smallint",
        "SELECT 9223372036854775807 + 1",
        "SELECT 1 / 0",
        "SELECT 1.0 / 0",
        "SELECT 'NaN'::numeric, 'Infinity'::float8, '-inf'::float8 < 0, 'nan'::float8 = 'nan'::float8",
        "SELECT round(2.5), round(-2.5), round(2.5::float8), trunc(-2.7), ceil(-0.5), floor(-0.5), round(1234.5678, -2)",
        "SELECT '12abc'::int",
        "SELECT ' 42 '::int, '1e3'::float8::int, 2.5::int, 3.5::int, (-2.5)::int",
        "SELECT 10::numeric(5,2), 123.456::numeric(5,2), 0.1 + 0.2 = 0.3, 0.1::float8 + 0.2::float8 = 0.3::float8",
        "SELECT 12345.678::numeric(4,1)",
        "SELECT abs(-5), sign(-3.2), mod(-7, 3), power(2, 10), sqrt(2::numeric), exp(0), ln(1), log(100), 5 ^ 2, |/ 25, @ -4",
        "SELECT 5 & 3, 5 | 3, 5 # 3, ~5, 1 << 4, 256 >> 2",
        "SELECT greatest(1, NULL, 3), least('b', 'a', NULL), coalesce(NULL, NULL), nullif(1, 1), nullif(1, 2)",
        "SELECT '1'::boolean, 'yes'::boolean, 'off'::boolean, 't'::boolean AND NULL, NULL OR true, NOT NULL::boolean",
        "SELECT 'maybe'::boolean",
    ),
    "pg edge: strings": steps(
        "SELECT length('héllo'), octet_length('héllo'), upper('ß'), lower('AB'), initcap('hello wORLD foo-bar')",
        "SELECT substr('hello', 0, 3), substr('hello', -1, 3), substring('hello' from 2 for 2), substring('abc123' from '[0-9]+'), left('abc', -1), right('abc', -1)",
        "SELECT position('lo' in 'hello'), strpos('hello', 'z'), replace('aaa', 'a', 'bb'), translate('hello', 'el', 'ip'), repeat('ab', 3), repeat('x', -1)",
        "SELECT 'a' || NULL, concat('a', NULL, 'b'), concat_ws(',', 'a', NULL, 'b'), 'abc' || 1 || true",
        "SELECT lpad('x', 5, 'ab'), rpad('hello', 3), btrim('  x  '), ltrim('xxyz', 'x'), trim(both 'x' from 'xxaxx'), reverse('abc'), split_part('a,b,c', ',', 2), split_part('a,b', ',', 5)",
        r"SELECT 'abc' LIKE 'a%', 'ABC' ILIKE 'a_c', 'a_c' LIKE 'a\_c', 'abc' SIMILAR TO 'a(b|x)c', 'abc' ~ '^a.c$', 'ABC' ~* 'abc', 'abc' !~ 'z'",
        "SELECT regexp_replace('a1b22c333', '[0-9]+', '#', 'g'), regexp_matches('foo123bar45', '([a-z]+)([0-9]+)'), regexp_split_to_array('a1b2c', '[0-9]')",
        "SELECT format('%s-%I-%L', 'x', 'my col', 'it''s'), quote_ident('Foo'), quote_literal(NULL), quote_nullable(NULL), md5('abc'), to_hex(255), ascii('A'), chr(66)",
        "SELECT 'abc' < 'abd', 'Z' < 'a', ''::text = ''",
        "SELECT string_agg(x, ',' ORDER BY x DESC) FROM (VALUES ('b'), ('a'), ('c')) t(x)",
        r"SELECT E'a\tb', 'a\tb', $$it's$$, U&'\0041'",
    ),
    "pg edge: dates and times": steps(
        "SELECT '2024-02-29'::date + 365, '2024-01-31'::date + interval '1 month', '2024-03-31'::date - interval '1 month'",
        "SELECT '2023-02-29'::date",
        "SELECT date_trunc('month', timestamp '2024-05-17 13:45:10'), date_trunc('week', timestamp '2024-05-17 13:45:10'), extract(dow from date '2024-05-19'), extract(epoch from timestamp '1970-01-02 00:00:00'), date_part('doy', date '2024-12-31')",
        "SELECT age(timestamp '2024-03-01', timestamp '2023-01-15'), timestamp '2024-01-01' - timestamp '2023-12-25 12:00', interval '1 day 25 hours', interval '90 minutes' * 2, justify_hours(interval '27 hours')",
        "SELECT to_char(timestamp '2024-05-07 09:05:03', 'YYYY-MM-DD HH24:MI:SS Dy Mon'), to_date('07/05/2024', 'DD/MM/YYYY'), make_date(2024, 2, 29), make_interval(days => 3)",
        "SELECT date '2024-01-01' < timestamp '2024-01-01 00:00:01', '2024-01-01'::date = '2024-01-01 00:00'::timestamp, (date '2024-01-10' - date '2024-01-01')",
        "SELECT '24:00:00'::time, 'allballs'::time, 'epoch'::timestamp, 'infinity'::timestamp > '2024-01-01'::timestamp",
        "SELECT generate_series(timestamp '2024-01-01', timestamp '2024-01-03', interval '1 day')",
        "SELECT timestamp '2024-01-01 12:00' AT TIME ZONE 'Asia/Kolkata', timestamptz '2024-01-01 12:00+05:30' AT TIME ZONE 'UTC'",
    ),
    "pg edge: arrays and json": steps(
        "SELECT ARRAY[1,2,3] || 4, array_cat(ARRAY[1], ARRAY[2,3]), array_append(NULL::int[], 1), (ARRAY[1,2,3])[2], (ARRAY[1,2,3])[5], (ARRAY[1,2,3])[2:3], cardinality(ARRAY[[1,2],[3,4]]), array_length(ARRAY[]::int[], 1)",
        "SELECT 2 = ANY(ARRAY[1,2]), 5 = ALL(ARRAY[5,5]), NULL = ANY(ARRAY[1]), 3 = ANY(ARRAY[1, NULL]), ARRAY[1,2] @> ARRAY[2], ARRAY[1,2] && ARRAY[3], array_position(ARRAY['a','b'], 'b'), array_remove(ARRAY[1,2,1], 1)",
        "SELECT unnest(ARRAY[3,1,2]) ORDER BY 1",
        """SELECT array_agg(x ORDER BY x), array_to_string(ARRAY[1,NULL,3], ',', '*'), string_to_array('a,,b', ','), '{1,2,3}'::int[] FROM (VALUES (2), (1)) t(x)""",
        """SELECT '{"a":{"b":[1,2]}}'::jsonb -> 'a' -> 'b' ->> 1, '{"a":1}'::jsonb ? 'a', '{"a":1,"b":2}'::jsonb - 'a', '[1,2,3]'::jsonb - 0, '{"a":1}'::jsonb || '{"b":2}', '{"a":[1]}'::jsonb #> '{a,0}'""",
        """SELECT jsonb_set('{"a":1}', '{b,c}', '2'), jsonb_set('{"a":1}', '{a}', '"x"'), jsonb_insert('[1,2]', '{1}', '9'), '{"a":1}'::jsonb @> '{}', jsonb_typeof('null'), json_typeof('1.5'), '{"b":1,"a":2}'::jsonb::text, '{"b":1,"a":2}'::json::text""",
        """SELECT jsonb_agg(x), jsonb_object_agg(k, x), json_build_object('a', 1, 'b', NULL), to_jsonb(ARRAY[1,2]) FROM (VALUES ('k1', 1)) t(k, x)""",
        """SELECT key, value FROM jsonb_each('{"a":1,"b":"x"}') ORDER BY key""",
        """SELECT '{"a": 1, "a": 2}'::jsonb, '{"a": 1, "a": 2}'::json, '1.0'::jsonb, '1e2'::jsonb, '[1, 2]'::jsonb = '[1,2]'::jsonb""",
        "SELECT 'not json'::jsonb",
    ),
    "pg edge: DML corner cases": steps(
        "CREATE TABLE t (id serial PRIMARY KEY, k text UNIQUE, n int NOT NULL DEFAULT 0, CHECK (n >= 0))",
        "INSERT INTO t (k, n) VALUES ('a', 1), ('b', 2) RETURNING id, k, n",
        "INSERT INTO t (k, n) VALUES ('a', 5) ON CONFLICT (k) DO UPDATE SET n = t.n + excluded.n RETURNING id, n",
        "INSERT INTO t (k, n) VALUES ('a', 5) ON CONFLICT (k) DO NOTHING RETURNING id",
        "INSERT INTO t (k, n) VALUES ('c', -1)",
        "INSERT INTO t (k) VALUES (NULL), (NULL) RETURNING k",
        "UPDATE t SET n = n + 1 WHERE k IS NULL RETURNING id, n",
        "DELETE FROM t WHERE k = 'zzz' RETURNING *",
        "UPDATE t SET n = -5 WHERE k = 'a'",
        "UPDATE t SET k = 'b' WHERE k = 'a'",
        "INSERT INTO t (id, k) VALUES (100, 'x'), (100, 'y')",
        "SELECT count(*) FROM t",
        "WITH d AS (DELETE FROM t WHERE k = 'b' RETURNING id) SELECT count(*) FROM d",
        "INSERT INTO t (k, n) SELECT 'g' || g, g FROM generate_series(1, 3) g RETURNING k",
        "UPDATE t SET n = s.v FROM (VALUES ('g1', 10), ('g2', 20)) s(k, v) WHERE t.k = s.k RETURNING t.k, t.n",
        "DELETE FROM t USING (VALUES ('g3')) s(k) WHERE t.k = s.k RETURNING t.k",
        "SELECT k, n FROM t ORDER BY k NULLS FIRST, n DESC",
        "TRUNCATE t RESTART IDENTITY",
        "INSERT INTO t (k) VALUES ('after') RETURNING id",
    ),
    "pg edge: queries": steps(
        "CREATE TABLE e (id int, dept text, sal numeric)",
        "INSERT INTO e VALUES (1,'a',100),(2,'a',200),(3,'b',150),(4,'b',NULL),(5,NULL,50)",
        "SELECT DISTINCT ON (dept) dept, id, sal FROM e ORDER BY dept, sal DESC NULLS LAST",
        "SELECT dept, count(*), count(sal), sum(sal), avg(sal), max(sal) FILTER (WHERE id > 1) FROM e GROUP BY dept ORDER BY dept NULLS LAST",
        "SELECT dept, sum(sal) FROM e GROUP BY ROLLUP (dept) ORDER BY dept NULLS LAST, 2",
        "SELECT dept, id > 2 AS big, count(*) FROM e GROUP BY CUBE (dept, id > 2) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3",
        "SELECT dept, count(*) FROM e GROUP BY GROUPING SETS ((dept), ()) ORDER BY 1 NULLS LAST, 2",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sal), percentile_disc(ARRAY[0.25, 0.75]) WITHIN GROUP (ORDER BY id DESC), percentile_cont(0.9) WITHIN GROUP (ORDER BY sal) FILTER (WHERE dept = 'a') FROM e",
        "SELECT dept, mode() WITHIN GROUP (ORDER BY sal DESC) FROM e GROUP BY dept ORDER BY 1 NULLS LAST",
        "SELECT percentile_cont(2) WITHIN GROUP (ORDER BY sal) FROM e",
        "SELECT id, rank() OVER w, sum(sal) OVER (w ROWS UNBOUNDED PRECEDING), lag(id) OVER w FROM e WINDOW w AS (PARTITION BY dept ORDER BY sal) ORDER BY id",
        "SELECT x, y FROM generate_series(1,2) x CROSS JOIN LATERAL (SELECT x * 10 AS y) l",
        "SELECT * FROM (VALUES (1, 'a'), (2, NULL)) v(n, s) WHERE s IS DISTINCT FROM 'a'",
        "SELECT id FROM e WHERE sal > ALL (SELECT sal FROM e WHERE dept = 'a') ORDER BY id",
        "SELECT id FROM e WHERE (dept, sal) = ('a', 200)",
        "SELECT id FROM e WHERE dept NOT IN ('a', NULL)",
        "SELECT count(*) FILTER (WHERE sal IS NULL), percentile_cont(0.5) WITHIN GROUP (ORDER BY sal), mode() WITHIN GROUP (ORDER BY dept) FROM e",
        "SELECT CASE WHEN sal > 100 THEN 'hi' WHEN sal IS NULL THEN 'none' END, id FROM e ORDER BY id",
        "SELECT id FROM e ORDER BY sal DESC NULLS FIRST LIMIT 2 OFFSET 1",
        "SELECT id FROM e ORDER BY id FETCH FIRST 2 ROWS ONLY",
        "SELECT 1 UNION SELECT 1.5 UNION SELECT NULL ORDER BY 1",
        "SELECT 'a' UNION SELECT 1",
        "SELECT exists(SELECT 1 FROM e WHERE sal IS NULL), (SELECT max(id) FROM e), (SELECT id FROM e WHERE false)",
        "SELECT dept FROM e GROUP BY dept HAVING count(*) > 1 ORDER BY 1",
        "SELECT id FROM e GROUP BY id HAVING sal > 1",
    ),
}

PG_EDGES.update({
    "pg plpgsql: functions": steps(
        """CREATE FUNCTION add(a int, b int DEFAULT 10) RETURNS int AS $$ BEGIN RETURN a + b; END $$ LANGUAGE plpgsql""",
        "SELECT add(1, 2), add(5), add(NULL, 1)",
        """CREATE FUNCTION sq(x numeric) RETURNS numeric LANGUAGE sql IMMUTABLE AS $$ SELECT x * x $$""",
        "SELECT sq(1.5), sq(NULL)",
        """CREATE FUNCTION fact(n int) RETURNS bigint LANGUAGE plpgsql AS $$
           DECLARE r bigint := 1; i int;
           BEGIN
             IF n < 0 THEN RAISE EXCEPTION 'negative: %', n USING ERRCODE = '22023'; END IF;
             FOR i IN 1..n LOOP r := r * i; END LOOP;
             RETURN r;
           END $$""",
        "SELECT fact(0), fact(5), fact(20)",
        "SELECT fact(-1)",
        """CREATE FUNCTION classify(x int) RETURNS text LANGUAGE plpgsql AS $$
           BEGIN
             CASE WHEN x < 0 THEN RETURN 'neg'; WHEN x = 0 THEN RETURN 'zero'; ELSE RETURN 'pos'; END CASE;
           END $$""",
        "SELECT classify(-3), classify(0), classify(7)",
        """CREATE FUNCTION loopy(n int) RETURNS text LANGUAGE plpgsql AS $$
           DECLARE s text := ''; i int := 0;
           BEGIN
             <<outer>> LOOP
               i := i + 1;
               CONTINUE WHEN i % 2 = 0;
               EXIT outer WHEN i > n;
               s := s || i::text || ',';
             END LOOP;
             WHILE length(s) > 0 AND right(s, 1) = ',' LOOP s := left(s, -1); END LOOP;
             RETURN s;
           END $$""",
        "SELECT loopy(7), loopy(0)",
        "CREATE TABLE acct (id int PRIMARY KEY, bal numeric NOT NULL CHECK (bal >= 0))",
        "INSERT INTO acct VALUES (1, 100), (2, 50)",
        """CREATE FUNCTION transfer(a int, b int, amt numeric) RETURNS text LANGUAGE plpgsql AS $$
           DECLARE n int;
           BEGIN
             UPDATE acct SET bal = bal - amt WHERE id = a;
             GET DIAGNOSTICS n = ROW_COUNT;
             IF n = 0 THEN RAISE EXCEPTION 'no account %', a; END IF;
             UPDATE acct SET bal = bal + amt WHERE id = b;
             RETURN 'ok';
           EXCEPTION
             WHEN check_violation THEN RETURN 'insufficient funds';
           END $$""",
        "SELECT transfer(1, 2, 30)",
        "SELECT transfer(1, 2, 500)",
        "SELECT id, bal FROM acct ORDER BY id",
        "SELECT transfer(9, 2, 1)",
        """CREATE FUNCTION lookup(k int) RETURNS numeric LANGUAGE plpgsql AS $$
           DECLARE v numeric;
           BEGIN
             SELECT bal INTO STRICT v FROM acct WHERE id = k;
             RETURN v;
           EXCEPTION WHEN no_data_found THEN RETURN -1;
           END $$""",
        "SELECT lookup(1), lookup(99)",
        """CREATE FUNCTION evens(n int) RETURNS SETOF int LANGUAGE plpgsql AS $$
           BEGIN FOR i IN 1..n LOOP IF i % 2 = 0 THEN RETURN NEXT i; END IF; END LOOP; END $$""",
        "SELECT * FROM evens(9)",
        """CREATE FUNCTION accts(min numeric) RETURNS TABLE(aid int, abal numeric) LANGUAGE plpgsql AS $$
           BEGIN RETURN QUERY SELECT id, bal FROM acct WHERE bal >= min ORDER BY id; END $$""",
        "SELECT * FROM accts(60)",
        """CREATE FUNCTION nums() RETURNS SETOF int LANGUAGE sql AS $$ SELECT generate_series(1, 3) $$""",
        "SELECT n * 10 FROM nums() AS n",
        "DO $$ DECLARE c int; BEGIN SELECT count(*) INTO c FROM acct; IF c <> 2 THEN RAISE EXCEPTION 'bad'; END IF; END $$",
        "DO $$ BEGIN PERFORM 1 / 0; EXCEPTION WHEN division_by_zero THEN INSERT INTO acct VALUES (3, 0); END $$",
        "SELECT count(*) FROM acct",
        "CREATE PROCEDURE add_acct(i int, b numeric) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO acct VALUES (i, b); END $$",
        "CALL add_acct(4, 40)",
        "SELECT id, bal FROM acct ORDER BY id",
        "DROP FUNCTION add(int, int)",
        "SELECT add(1, 2)",
        "DROP FUNCTION IF EXISTS nosuch(int)",
        "CREATE FUNCTION bad() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1 END $$",
        "CREATE FUNCTION noret() RETURNS int LANGUAGE plpgsql AS $$ BEGIN NULL; END $$",
        "SELECT noret()",
    ),
    "pg plpgsql: triggers": steps(
        "CREATE TABLE item (id serial PRIMARY KEY, name text NOT NULL, price numeric, updated_at timestamp, version int NOT NULL DEFAULT 0)",
        "CREATE TABLE audit (id serial PRIMARY KEY, op text, item_id int, old_name text, new_name text)",
        """CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN NEW.updated_at := '2024-01-01 00:00:00'; NEW.version := OLD.version + 1; RETURN NEW; END $$""",
        """CREATE FUNCTION log_item() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN
             IF TG_OP = 'DELETE' THEN
               INSERT INTO audit (op, item_id, old_name) VALUES (TG_OP, OLD.id, OLD.name);
               RETURN OLD;
             ELSIF TG_OP = 'UPDATE' THEN
               INSERT INTO audit (op, item_id, old_name, new_name) VALUES (TG_OP, NEW.id, OLD.name, NEW.name);
             ELSE
               INSERT INTO audit (op, item_id, new_name) VALUES (TG_OP, NEW.id, NEW.name);
             END IF;
             RETURN NEW;
           END $$""",
        """CREATE FUNCTION check_price() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN
             IF NEW.price < 0 THEN RAISE EXCEPTION 'price % is negative', NEW.price USING ERRCODE = 'check_violation'; END IF;
             IF NEW.name = 'skip' THEN RETURN NULL; END IF;
             NEW.name := trim(NEW.name);
             RETURN NEW;
           END $$""",
        "CREATE TRIGGER a_check BEFORE INSERT OR UPDATE ON item FOR EACH ROW EXECUTE FUNCTION check_price()",
        "CREATE TRIGGER b_touch BEFORE UPDATE ON item FOR EACH ROW EXECUTE FUNCTION touch()",
        "CREATE TRIGGER z_log AFTER INSERT OR UPDATE OR DELETE ON item FOR EACH ROW EXECUTE FUNCTION log_item()",
        "INSERT INTO item (name, price) VALUES ('  apple ', 1.5), ('skip', 2), ('pear', 3) RETURNING id, name, version",
        "INSERT INTO item (name, price) VALUES ('bad', -1)",
        "UPDATE item SET price = price * 2 WHERE name = 'apple' RETURNING name, price, updated_at, version",
        "UPDATE item SET name = 'skip'",
        "SELECT id, name, price, version FROM item ORDER BY id",
        "DELETE FROM item WHERE name = 'pear'",
        "SELECT op, item_id, old_name, new_name FROM audit ORDER BY id",
        """CREATE FUNCTION keep_one() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN IF (SELECT count(*) FROM item) <= 1 THEN RETURN NULL; END IF; RETURN OLD; END $$""",
        "CREATE TRIGGER guard BEFORE DELETE ON item FOR EACH ROW EXECUTE FUNCTION keep_one()",
        "DELETE FROM item",
        "SELECT count(*) FROM item",
        "CREATE TABLE cnt (n int)",
        "INSERT INTO cnt VALUES (0)",
        """CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE cnt SET n = n + 1; RETURN NULL; END $$""",
        "CREATE TRIGGER per_stmt AFTER UPDATE ON item FOR EACH STATEMENT EXECUTE FUNCTION bump()",
        "CREATE TRIGGER only_price AFTER UPDATE OF price ON item FOR EACH ROW WHEN (NEW.price > 100) EXECUTE FUNCTION bump()",
        "UPDATE item SET name = name",
        "UPDATE item SET price = 500",
        "UPDATE item SET price = 1 WHERE false",
        "SELECT n FROM cnt",
        "DROP TRIGGER per_stmt ON item",
        "DROP TRIGGER per_stmt ON item",
        "DROP FUNCTION bump()",
        "CREATE FUNCTION not_trigger() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        "CREATE TRIGGER t BEFORE INSERT ON item FOR EACH ROW EXECUTE FUNCTION not_trigger()",
        "CREATE TABLE rec (n int)",
        "CREATE FUNCTION again() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rec VALUES (NEW.n + 1); RETURN NEW; END $$",
        "CREATE TRIGGER loop_t AFTER INSERT ON rec FOR EACH ROW WHEN (NEW.n < 5) EXECUTE FUNCTION again()",
        "INSERT INTO rec VALUES (1)",
        "SELECT n FROM rec ORDER BY n",
    ),
})
