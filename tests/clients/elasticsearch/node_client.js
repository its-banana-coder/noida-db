// Official @elastic/elasticsearch 8.x smoke test against noida-db.
'use strict';

const { Client } = require('@elastic/elasticsearch');

async function main() {
  const index = 'noida_node_client';
  const client = new Client({ node: process.env.ELASTICSEARCH_URL });
  const info = await client.info();
  if (info.version.number !== '8.15.3') throw new Error(`unexpected version: ${info.version.number}`);
  await client.indices.delete({ index }, { ignore: [404] });
  const created = await client.indices.create({ index });
  if (!created.acknowledged) throw new Error('index creation was not acknowledged');
  const written = await client.index({ index, id: 'book-1', document: { title: 'Dune', pages: 412 } });
  if (written.result !== 'created') throw new Error(`index result: ${written.result}`);
  const found = await client.get({ index, id: 'book-1' });
  if (found._source.title !== 'Dune' || found._source.pages !== 412) {
    throw new Error(`unexpected source: ${JSON.stringify(found._source)}`);
  }
  await client.indices.delete({ index });
  console.log('@elastic/elasticsearch: info/index/get passed');
}

main().catch((error) => {
  console.error(`@elastic/elasticsearch FAILED: ${error.message}`);
  process.exit(1);
});
