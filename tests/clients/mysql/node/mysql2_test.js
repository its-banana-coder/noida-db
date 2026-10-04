// mysql2 (server-side prepared statements via execute()) against noida-db.
const mysql = require('mysql2/promise');
const PORT = Number(process.env.NOIDA_MYSQL_PORT || 3306);
let pass = 0; const fail = [];
async function check(name, fn, want) {
  try {
    const got = await fn();
    const g = JSON.stringify(got), w = JSON.stringify(want);
    if (g === w) pass++; else fail.push(`${name}\n    got  ${g}\n    want ${w}`);
  } catch (e) { fail.push(`${name}\n    ERR ${e.code || ''} ${e.message}`); }
}
(async () => {
  const opts = { host: '127.0.0.1', port: PORT, user: 'root', database: 'test' };
  const c = await mysql.createConnection({ ...opts, supportBigNumbers: true, dateStrings: true });
  await c.query('CREATE TABLE p (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20) NOT NULL, price DECIMAL(8,2), qty INT, born DATE, at DATETIME, meta JSON, ok TINYINT(1))');
  await check('execute insert (binary protocol)', async () => {
    const [r] = await c.execute('INSERT INTO p (name, price, qty, born, at, meta, ok) VALUES (?, ?, ?, ?, ?, ?, ?)',
      ['apple', '9.99', 3, '2024-02-29', '2024-03-05 14:07:09', JSON.stringify({ a: [1, 2] }), true]);
    return [r.affectedRows, r.insertId];
  }, [1, 1]);
  await c.execute('INSERT INTO p (name, price, qty) VALUES (?, ?, ?)', ['pear', '0.10', null]);
  await check('typed row via execute', async () => (await c.execute('SELECT id, name, price, qty, born, at, meta, ok FROM p WHERE id = ?', [1]))[0][0],
    { id: 1, name: 'apple', price: '9.99', qty: 3, born: '2024-02-29', at: '2024-03-05 14:07:09', meta: { a: [1, 2] }, ok: 1 });
  await check('NULL param + IS NULL', async () => (await c.execute('SELECT name FROM p WHERE qty IS NULL AND price > ?', [0.05]))[0].map(r => r.name), ['pear']);
  await check('aggregate DECIMAL', async () => (await c.execute('SELECT SUM(price) AS s, COUNT(*) AS n FROM p'))[0][0], { s: '10.09', n: 2 });
  await check('LIMIT ? param', async () => (await c.execute('SELECT name FROM p WHERE name LIKE ? ORDER BY id LIMIT ?', ['%p%', '1']))[0].map(r => r.name), ['apple']);
  await check('param order: SELECT list then WHERE', async () => (await c.execute('SELECT ? AS a, name FROM p WHERE id = ?', ['X', 2]))[0], [{ a: 'X', name: 'pear' }]);
  await check('HAVING ? param', async () => (await c.execute('SELECT name, COUNT(*) AS n FROM p GROUP BY name HAVING COUNT(*) >= ? ORDER BY name', [1]))[0].length, 2);
  await check('query() with array expansion', async () => (await c.query('SELECT name FROM p WHERE id IN (?) ORDER BY id', [[1, 2]]))[0].map(r => r.name), ['apple', 'pear']);
  await check('prepared statement reuse', async () => {
    const out = [];
    for (let i = 0; i < 20; i++) { const [r] = await c.execute('SELECT ? + 1 AS v', [i]); out.push(Number(r[0].v)); }
    return out.slice(-3);
  }, [18, 19, 20]);
  await check('dup key error code', async () => {
    await c.query('CREATE TABLE u (k VARCHAR(5) PRIMARY KEY)');
    await c.execute('INSERT INTO u VALUES (?)', ['a']);
    try { await c.execute('INSERT INTO u VALUES (?)', ['a']); return 'no error'; } catch (e) { return [e.code, e.errno]; }
  }, ['ER_DUP_ENTRY', 1062]);
  await check('upsert via execute', async () => {
    const [r] = await c.execute('INSERT INTO p (id, name, qty) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)', [1, 'apple', 10]);
    return [r.affectedRows, (await c.execute('SELECT qty FROM p WHERE id = 1'))[0][0].qty];
  }, [2, 13]);
  await check('beginTransaction / rollback', async () => {
    await c.beginTransaction(); await c.execute('DELETE FROM p'); await c.rollback();
    return (await c.execute('SELECT COUNT(*) AS n FROM p'))[0][0].n;
  }, 2);
  await check('pool + 20 concurrent queries', async () => {
    const pool = mysql.createPool({ ...opts, connectionLimit: 5 });
    const rs = await Promise.all([...Array(20).keys()].map(i => pool.execute('SELECT ? AS v', [i])));
    await pool.end();
    return rs.map(([r]) => Number(r[0].v)).reduce((a, b) => a + b, 0);
  }, 190);
  await check('DATETIME as Date object', async () => {
    const c2 = await mysql.createConnection({ ...opts, timezone: 'Z' });
    const [rows] = await c2.execute('SELECT at FROM p WHERE id = 1'); await c2.end();
    return rows[0].at.toISOString();
  }, '2024-03-05T14:07:09.000Z');
  await check('DATETIME param round trip', async () => {
    await c.execute('UPDATE p SET at = ? WHERE id = 2', [new Date(Date.UTC(2025, 0, 2, 3, 4, 5))]);
    return (await c.execute('SELECT at FROM p WHERE id = 2'))[0][0].at.slice(0, 10);
  }, '2025-01-02');
  await c.end();
  console.log(`mysql2: ${pass}/${pass + fail.length} checks passed`);
  fail.forEach(f => console.log('  FAIL ' + f));
  process.exit(fail.length ? 1 : 0);
})().catch(e => { console.error('mysql2 FAILED:', e); process.exit(1); });
