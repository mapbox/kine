'use strict';

const test = require('tape');
const Db = require('../lib/db2.js');
const Dyno = require('@mapbox/dyno');
const Dynalite = require('dynalite');
const dynaliteClient = Dynalite({createTableMs: 0});
const table = require('../lib/table.js');
const kUtils = require('@mapbox/kinesalite-utils')();
const queue = require('d3-queue').queue;

process.env.AWS_REGION = 'us-east-1';
process.env.AWS_ACCESS_KEY_ID = 'does not matter';
process.env.AWS_SECRET_ACCESS_KEY = 'does not matter';

const config = {
  table: 'kine-kcl-test',
  streamName: 'test-stream'
};

const fakeStream = {
  test: {
    name: config.streamName,
    shards: 8
  }
};

let dynoClient;
let db;

test('init resources (dynalite, dyno, db)', (t) => {
  dynaliteClient.listen(4568, function (err) {
    if (err) t.fail(err);
    t.error(err);
    console.log('Dynalite started on port 4568');
    dynoClient = Dyno({
      endpoint: 'http://localhost:4568',
      accessKeyId: 'fake',
      secretAccessKey: 'fake',
      region: 'us-east-1',
      table: config.table
    });
    db = Db(config, dynoClient, kUtils.kClient);
    return kUtils.startServer(fakeStream)
      .then(kUtils.listKinesisStreams)
      .then(setTimeout(t.end, 200))
      .catch((err) => t.fail(err.stack || err));
  });
});

test('create initial table', (t) => {
  db.createTable((err, data) => {
    t.deepEqual(table(config.table).AttributeDefinitions, data.TableDefinition.AttributeDefinitions, 'attribute definitions are the same');
    t.deepEqual(table(config.table).KeySchema, data.TableDefinition.KeySchema, 'key schema is the same');
    t.deepEqual(table(config.table).TableName, config.table, 'table name is the same');
    t.end();
  });
});

test('stream description is updated to dynamo table', (t) => {
  let q = queue(1);
  q.defer(db.updateShards);
  q.defer(db.getShardList);
  q.await((err, streamShards, dynamoState) => {
    if (err) t.fail(err);
    t.equal(streamShards.length, dynamoState.length, 'state row count is the same as shard count');
    for (let i = 0; i < streamShards.length; i++) {
      t.equal(streamShards[i].ShardId, dynamoState[i].id, 'shard exists in state table');
    }
    t.end();
  });
});

test('teardown resources', (t) => {
  kUtils.stopServer();
  dynaliteClient.close(function (err) {
    t.error(err);
    console.log('Dynalite closed');
    t.end();
  });
});