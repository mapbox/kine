'use strict';

const Sync = require('../lib/sync.js');
const test = require('tape');
const Dyno = require('@mapbox/dyno');
const util = require('./utils');
const AWS = require('aws-sdk');

const config = {
  verbose: true,
  table: 'kine-kcl-test',
  streamName: 'teststream'
};

const kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568'
};

const kinesisClient = new AWS.Kinesis(kinesisOptions);

test('init', util.init);

const dynoClient = Dyno({
  endpoint: 'http://localhost:4568',
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  table: config.table
});

test('shard syncing to state table', (t) => {
  let sync = Sync(config, dynoClient, kinesisClient);
  sync.shardSyncDelay = 1000; // speed up shard syncing for testing
  let syncIndex = 0;

  function dynamoStateEqualsStreamState(stream, dynamo) {
    for (let i = 0; i < stream.length; i++) {
      t.equal(stream[i].ShardId, dynamo[i].id, `id is the same for ${dynamo[i].id}`);
      t.equal(stream[i].HashKeyRange.StartingHashKey, dynamo[i].hashKeyStart, `starting hash key is the same for ${dynamo[i].hashKeyStart}`);
      t.equal(stream[i].HashKeyRange.EndingHashKey, dynamo[i].hashKeyEnd, `ending hash key is the same for ${dynamo[i].hashKeyEnd}`);
    }
  }

  // make sure we have a state table
  sync.init(function () {
    sync.events.on('shardSync', (state) => {
      t.ok('shards have synced');
      // first sync, PUT leases based on shards
      if (syncIndex === 0) {
        t.equal(state.stream.length, state.state.length, 'same amount of rows');
        for (let i = 0; i < state.stream.length; i++) {
          let shardRow = state.stream[i];
          let dynamoRow = state.state[i];
          t.equal(shardRow.ShardId, dynamoRow.Attributes.id, `shardId ${shardRow.ShardId} matches`);
          t.equal(dynamoRow.Attributes.status, 'available', `shardId ${shardRow.ShardId} is available`);
        }
        // get it from dynamo just to be sure
        sync.getShardList(function (err, dynamoResult) {
          if (err) t.fail(err);
          dynamoStateEqualsStreamState(state.stream, dynamoResult);
        });
      }
      // second sync, nothing should have changed
      if (syncIndex === 1) {
        t.equal(undefined, state.state, 'nothing changed, state is undefined');
        clearTimeout(sync.shardSyncTimeout);
        t.end();
      }
      syncIndex++;
    });
    sync.shards();
  });
});

test('teardown', util.teardown);