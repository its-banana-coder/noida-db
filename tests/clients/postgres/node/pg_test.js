// node-postgres against noida: parameterized CRUD, types, transactions,
// pooling and catalog introspection.
'use strict';

const { Client, Pool, types } = require('pg');

const port = parseInt(process.env.PGPORT || '5432', 10);
const config = {
  host: '127.0.0.1',
  port,
  user: 'postgres',
  password: 'postgres',
  database: 'postgres',
};

let checks = 0;
function check(what, got, want) {
  checks += 1;
  const g = JSON.stringify(got);
  const w = JSON.stringify(want);
  if (g !== w) {
    throw new Error(`${what}: got ${g}, want ${w}`);
  }
}

async function main() {
  const c = new Client(config);
  await c.connect();

  await c.query('DROP TABLE IF EXISTS notes');
  await c.query(`CREATE TABLE notes (
      id serial PRIMARY KEY,
      title text NOT NULL,
      body text,
      n int,
      ratio float8,
      ok bool,
      tags text[],
      meta jsonb,
      created timestamptz DEFAULT now()
  )`);

  // Parameterized insert (extended query protocol) with RETURNING.
  const ins = await c.query(
    'INSERT INTO notes (title, body, n, ratio, ok, tags, meta) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, ok',
    ['first', 'hello', 7, 1.5, true, ['x', 'y'], { a: 1, b: [2, 3] }],
  );
  check('returning', ins.rows[0], { id: 1, ok: true });
  check('rowCount', ins.rowCount, 1);
  check('command', ins.command, 'INSERT');

  // Types round-trip through the driver's parsers.
  const sel = await c.query('SELECT * FROM notes WHERE id = $1', [1]);
  const row = sel.rows[0];
  check('text', row.title, 'first');
  check('int', row.n, 7);
  check('float8', row.ratio, 1.5);
  check('bool', row.ok, true);
  check('array', row.tags, ['x', 'y']);
  check('jsonb', row.meta, { a: 1, b: [2, 3] });
  check('timestamptz is a Date', row.created instanceof Date, true);

  // Field metadata drives every ORM built on node-postgres.
  const nameField = sel.fields.find((f) => f.name === 'title');
  check('field oid', nameField.dataTypeID, 25);
  const idField = sel.fields.find((f) => f.name === 'id');
  check('id oid', idField.dataTypeID, 23);

  // NULLs, updates and deletes.
  await c.query('INSERT INTO notes (title, body) VALUES ($1, $2)', ['second', null]);
  const nulls = await c.query('SELECT body FROM notes WHERE title = $1', ['second']);
  check('null', nulls.rows[0].body, null);
  const upd = await c.query('UPDATE notes SET n = COALESCE(n, 0) + $1 WHERE title = $2', [5, 'second']);
  check('update count', upd.rowCount, 1);
  const del = await c.query('DELETE FROM notes WHERE title = $1', ['nope']);
  check('delete count', del.rowCount, 0);

  // Transactions.
  await c.query('BEGIN');
  await c.query("INSERT INTO notes (title) VALUES ('rolled back')");
  await c.query('ROLLBACK');
  const count = await c.query('SELECT count(*)::int AS n FROM notes');
  check('after rollback', count.rows[0].n, 2);

  // Errors expose the SQLSTATE, and the connection survives.
  try {
    await c.query('SELECT * FROM missing_table');
    throw new Error('expected an error');
  } catch (e) {
    check('undefined table', e.code, '42P01');
  }
  const still = await c.query('SELECT 1 AS one');
  check('usable after error', still.rows[0].one, 1);

  try {
    await c.query('INSERT INTO notes (id, title) VALUES ($1, $2)', [1, 'dup']);
    throw new Error('expected a unique violation');
  } catch (e) {
    check('unique violation', e.code, '23505');
    check('constraint name', e.constraint, 'notes_pkey');
  }

  // Catalog introspection, as schema tools do it.
  const cols = await c.query(
    `SELECT column_name, data_type FROM information_schema.columns
     WHERE table_name = 'notes' ORDER BY ordinal_position`,
  );
  check('columns', cols.rows.length, 9);
  check('first column', cols.rows[0], { column_name: 'id', data_type: 'integer' });

  // A prepared (named) statement reused with different parameters.
  for (const [arg, want] of [[0, 2], [7, 1], [100, 0]]) {
    const r = await c.query({
      name: 'by_n',
      text: 'SELECT count(*)::int AS n FROM notes WHERE COALESCE(n,0) >= $1',
      values: [arg],
    });
    check(`prepared ${arg}`, r.rows[0].n, want);
  }

  await c.end();

  // A pool, the way web apps connect.
  const pool = new Pool({ ...config, max: 3 });
  const results = await Promise.all([
    pool.query('SELECT 1 AS a'),
    pool.query('SELECT count(*)::int AS n FROM notes'),
    pool.query('SELECT title FROM notes ORDER BY id LIMIT 1'),
  ]);
  check('pool select', results[0].rows[0].a, 1);
  check('pool count', results[1].rows[0].n, 2);
  check('pool row', results[2].rows[0].title, 'first');
  await pool.query('DROP TABLE notes');
  await pool.end();

  check('parser present', typeof types.getTypeParser(23), 'function');
  console.log(`node-postgres: ${checks} checks passed`);
}

main().catch((e) => {
  console.error(`node-postgres FAILED: ${e.message}`);
  process.exit(1);
});
