// Knex against noida-db: schema builder, transactions, joins, constraint
// violations. Run with PGPORT pointing at noida-db (or a real Postgres,
// which must pass just the same).
'use strict';

const knexLib = require('knex');
const port = process.env.PGPORT;
let checks = 0;
function check(cond, msg) { if (!cond) throw new Error('FAIL: ' + msg); checks++; }

async function main() {
  const knex = knexLib({ client: 'pg', connection: { host: '127.0.0.1', port, user: 'postgres', database: 'postgres' } });
  await knex.schema.createTable('users', (t) => {
    t.increments('id');
    t.string('name').notNullable();
    t.decimal('balance', 10, 2).defaultTo(0);
    t.jsonb('meta');
    t.timestamps(true, true);
  });
  await knex('users').insert([{ name: 'ann', balance: 5.5 }, { name: 'bob', meta: { k: 1 } }]);
  const rows = await knex('users').select('*').orderBy('id');
  check(rows.length === 2 && rows[0].name === 'ann', 'insert/select');
  const [{ count }] = await knex('users').count('* as count');
  check(Number(count) === 2, 'count');
  await knex.transaction(async (trx) => {
    await trx('users').where({ id: 1 }).update({ balance: 9.99 });
  });
  const u = await knex('users').where({ id: 1 }).first();
  check(Number(u.balance) === 9.99, 'transaction+update ' + u.balance);
  await knex.schema.alterTable('users', (t) => { t.string('email').unique(); });
  await knex('users').where({ id: 1 }).update({ email: 'a@x' });
  try {
    await knex('users').where({ id: 2 }).update({ email: 'a@x' });
    check(false, 'unique should fail');
  } catch (e) { check(true, 'unique violation'); }
  const joined = await knex('users as u').leftJoin('users as v', 'u.id', 'v.id').select('u.name').limit(1);
  check(joined.length === 1, 'join');
  await knex.destroy();
  console.log(`knex: ${checks} checks passed`);
}
main().catch((e) => { console.error('knex FAILED:', e.message); process.exit(1); });
