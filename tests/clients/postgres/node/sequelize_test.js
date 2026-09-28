// Sequelize against noida-db: model sync, associations (include), arrays
// and jsonb, operator filters, transactions (commit and rollback), and
// constraint violations. Run with PGPORT pointing at noida-db (or a real
// Postgres, which must pass just the same).
'use strict';
const { Sequelize, DataTypes, Op } = require('sequelize');

const port = parseInt(process.env.PGPORT || '5432', 10);
let checks = 0;
function check(cond, msg) { if (!cond) throw new Error('FAIL: ' + msg); checks++; }

async function main() {
  const sequelize = new Sequelize('postgres', 'postgres', undefined, {
    host: '127.0.0.1', port, dialect: 'postgres', logging: false,
  });
  await sequelize.authenticate();

  const Author = sequelize.define('SeqAuthor', {
    name: { type: DataTypes.STRING(60), unique: true, allowNull: false },
  });
  const Book = sequelize.define('SeqBook', {
    title: DataTypes.TEXT,
    price: DataTypes.DECIMAL(8, 2),
    tags: DataTypes.ARRAY(DataTypes.STRING),
    meta: DataTypes.JSONB,
  });
  Author.hasMany(Book);
  Book.belongsTo(Author);

  await sequelize.sync({ force: true });

  const ann = await Author.create({ name: 'Ann' });
  await Book.bulkCreate([
    { title: 'Alpha', price: 9.99, tags: ['x', 'y'], meta: { k: 1 }, SeqAuthorId: ann.id },
    { title: 'Beta', price: 19.5, SeqAuthorId: ann.id },
  ]);
  const count = await Book.count();
  check(count === 2, 'count ' + count);

  const withAuthor = await Book.findAll({ include: Author, order: [['title', 'ASC']] });
  check(withAuthor[0].SeqAuthor.name === 'Ann' && withAuthor[0].tags.length === 2, 'include+array');
  check(withAuthor[0].meta.k === 1, 'jsonb');

  const cheap = await Book.findAll({ where: { price: { [Op.lt]: 15 } } });
  check(cheap.length === 1 && cheap[0].title === 'Alpha', 'operator filter');

  const t = await sequelize.transaction();
  await Book.update({ price: 29.99 }, { where: { title: 'Alpha' }, transaction: t });
  await t.rollback();
  const afterRollback = await Book.findOne({ where: { title: 'Alpha' } });
  check(Number(afterRollback.price) === 9.99, 'rollback ' + afterRollback.price);

  const t2 = await sequelize.transaction();
  await Book.update({ price: 5.0 }, { where: { title: 'Beta' }, transaction: t2 });
  await t2.commit();
  const afterCommit = await Book.findOne({ where: { title: 'Beta' } });
  check(Number(afterCommit.price) === 5, 'commit');

  let dup = false;
  try {
    await Author.create({ name: 'Ann' });
  } catch (e) {
    dup = true;
  }
  check(dup, 'unique violation should error');

  await sequelize.close();
  console.log(`sequelize: ${checks} checks passed`);
}

main().catch((e) => { console.error('sequelize FAILED:', e.message); process.exit(1); });
