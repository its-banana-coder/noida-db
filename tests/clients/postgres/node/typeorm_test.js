// TypeORM against noida-db: entity schemas, synchronize (DDL from entities),
// relations, the query builder, and transactions. Run with PGPORT pointing
// at noida-db (or a real Postgres, which must pass just the same).
'use strict';

require('reflect-metadata');
const { DataSource, EntitySchema } = require('typeorm');
const port = process.env.PGPORT;
let checks = 0;
function check(cond, msg) { if (!cond) throw new Error('FAIL: ' + msg); checks++; }

const Author = new EntitySchema({
  name: 'Author',
  tableName: 'authors',
  columns: {
    id: { primary: true, type: 'int', generated: true },
    name: { type: 'varchar', length: 60, unique: true },
  },
  relations: {
    books: { type: 'one-to-many', target: 'Book', inverseSide: 'author' },
  },
});
const Book = new EntitySchema({
  name: 'Book',
  tableName: 'books',
  columns: {
    id: { primary: true, type: 'bigint', generated: true },
    title: { type: 'varchar' },
    price: { type: 'decimal', precision: 8, scale: 2, default: 0 },
    tags: { type: 'text', array: true, nullable: true },
  },
  relations: {
    author: { type: 'many-to-one', target: 'Author', joinColumn: true, inverseSide: 'books' },
  },
});

async function main() {
  const ds = new DataSource({
    type: 'postgres', host: '127.0.0.1', port, username: 'postgres', database: 'postgres',
    entities: [Author, Book], synchronize: true, logging: false,
  });
  await ds.initialize();
  const authorRepo = ds.getRepository('Author');
  const bookRepo = ds.getRepository('Book');
  const a = await authorRepo.save({ name: 'Ann' });
  await bookRepo.save([
    { title: 'Alpha', price: 9.99, tags: ['x', 'y'], author: a },
    { title: 'Beta', price: 19.5, author: a },
  ]);
  const count = await bookRepo.count();
  check(count === 2, 'count ' + count);
  const withAuthor = await bookRepo.find({ relations: { author: true }, order: { title: 'ASC' } });
  check(withAuthor[0].author.name === 'Ann' && withAuthor[0].tags.length === 2, 'relation+array');
  await ds.transaction(async (mgr) => {
    await mgr.getRepository('Book').update({ title: 'Alpha' }, { price: 29.99 });
  });
  const updated = await bookRepo.findOneBy({ title: 'Alpha' });
  check(Number(updated.price) === 29.99, 'transaction update ' + updated.price);
  const qb = await bookRepo.createQueryBuilder('b')
    .innerJoinAndSelect('b.author', 'a')
    .where('b.price > :p', { p: 20 })
    .getMany();
  check(qb.length === 1 && qb[0].title === 'Alpha', 'querybuilder ' + JSON.stringify(qb));
  try {
    await authorRepo.save({ name: 'Ann' });
    check(false, 'unique should fail');
  } catch (e) { check(true, 'unique violation'); }
  await ds.destroy();
  console.log(`typeorm: ${checks} checks passed`);
}
main().catch((e) => { console.error('typeorm FAILED:', e.message); process.exit(1); });
