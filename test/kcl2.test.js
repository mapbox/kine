'use strict';

const Kcl = require('../lib/kcl2.js');
const test = require('tape');
const Dyno = require('@mapbox/dyno');
const Dynalite = require('dynalite');
const dynaliteClient = Dynalite({createTableMs: 0});
const table = require('../lib/table.js');
const kUtils = require('@mapbox/kinesalite-utils')();
const queue = require('d3-queue').queue;
const AWS = require('aws-sdk');

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

const kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:4567'
};

const kinesisClient = new AWS.Kinesis(kinesisOptions);
let dynoClient;

test('init resources', (t) => {
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

      //
      // if (err) t.fail(err);
      // kUtils.startServer(fakeStream)
      //   .then(kUtils.listKinesisStreams)
      //   .then(setTimeout(t.end, 300))
      //   .catch((err) => t.fail(err.stack || err));

  });
});

test('shard syncing to state table', (t) => {
  // let kcl = Kcl(config, dynoClient, kinesisClient);
  // kcl.events.on('shardSync', (state) => {
  //   t.ok('shards have synced');
  //   clearTimeout(kcl.shardSyncTimeout);
  //   t.equal(state.stream.length, state.state.length, 'same amount of rows');
  //   for (let i = 0; i < state.stream.length; i++) {
  //     let shardRow = state.stream[i];
  //     let dynamoRow = state.state[i];
  //     t.equal(shardRow.ShardId, dynamoRow.Attributes.id, `shardId ${shardRow.ShardId} matches`);
  //     t.equal(dynamoRow.Attributes.status, 'available', `shardId ${shardRow.ShardId} is available`);
  //   }
  //   t.end();
  // });
  // kcl.shardSync();
  t.end();
});

test('teardown resources', (t) => {
  kUtils.stopServer();
  dynaliteClient.close(function (err) {
    t.error(err);
    console.log('Dynalite closed');
    t.end();
  });
});