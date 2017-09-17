'use strict';

const Kcl = require('../lib/kcl.js');
const Sync = require('../lib/sync.js');
const test = require('tape');
const Dyno = require('@mapbox/dyno');
const queue = require('d3-queue').queue;
const AWS = require('aws-sdk');
const util = require('./utils');

process.env.AWS_REGION = 'us-east-1';
process.env.AWS_ACCESS_KEY_ID = 'does not matter';
process.env.AWS_SECRET_ACCESS_KEY = 'does not matter';

const kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568'
};

const kinesisClient = new AWS.Kinesis(kinesisOptions);
const syncConfig = {
  verbose: true,
  table: 'kine-kcl-test',
  streamName: 'teststream'
};
const dynoClient = Dyno({
  endpoint: 'http://localhost:4568',
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  table: syncConfig.table
});

test('init', util.init);
const sync = Sync(syncConfig, dynoClient, kinesisClient);

test('setup a sync instance', (t) => {
  sync.init(function () { // init table
    sync.shards(); // sync once
    sync.events.on('shardSync', () => {
      clearTimeout(sync.shardSyncTimeout);
      t.end();
    })
  })
});

test('shard availability', (t) => {
  sync.getShardList(function (err, result) {
    const config = {
      verbose: true,
      streamName: 'teststream'
    };
    const kcl = Kcl(config, dynoClient, kinesisClient);

    function checkShardAvailability() {
      return new Promise(function (resolve, reject) {
        if (err) return reject(err);
        kcl.availableShard(function (err, shard) {
          t.equal(shard.status, 'available', 'shard is available');
          resolve();
        })
      })
    }

    function leaseAllShards() {
      return new Promise(function (resolve, reject) {
        const q = queue(10);
        result.forEach(function (shard) {
          q.defer(kcl.leaseShard, shard);
        });
        q.awaitAll(function (err) {
          if (err) return reject(err);
          resolve();
        });
      })
    }

    function checkAllLeased() {
      return new Promise(function (resolve, reject) {
        sync.getShardList(function (err, shards) {
          if (err) return reject(err);
          shards.forEach(function (shard) {
            t.equal(shard.status, 'leased', `shard ${shard.id} is leased`)
          });
          resolve();
        });
      })
    }

    checkShardAvailability() // check that shards are available
      .then(leaseAllShards) // lease them all
      .then(checkAllLeased) // make sure none are free anymore
      .then(() => util.resetState(sync, dynoClient))
      .then(() => t.end())
      .catch((err) => t.fail(err.stack || err));
  })
});

test('get a shard iterator', (t) => {
  const config = {
    verbose: true,
    table: 'kine-kcl-test',
    streamName: 'teststream'
  };

  const kinesisClientTest = new AWS.Kinesis(kinesisOptions);
  kinesisClientTest.getShardIterator = function (params) {
    t.equal(params.StreamName, 'teststream', 'stream name is correct');
    t.equal(params.ShardId, 'anId', 'shard id is correct');
    t.equal(params.ShardIteratorType, 'LATEST', 'shard iterator type is correct');

    kinesisClientTest.getShardIterator = function (params) {
      t.equal(params.StreamName, 'teststream', 'stream name is correct');
      t.equal(params.ShardId, 'anotherId', 'shard id is correct');
      t.equal(params.ShardIteratorType, 'AFTER_SEQUENCE_NUMBER', 'shard iterator type is correct');
      t.equal(params.StartingSequenceNumber, 'aSequenceNumber', 'extra param exists for sequence number');

      kinesisClientTest.getShardIterator = function (params) {
        t.equal(params.StreamName, 'teststream', 'stream name is correct');
        t.equal(params.ShardId, 'oneLastId', 'shard id is correct');
        t.equal(params.ShardIteratorType, 'AT_TIMESTAMP', 'shard iterator type is correct');
        t.equal(params.Timestamp, 'aGreatTimestamp', 'extra param exists for timestamp');
        t.end();
      };

      kcl.updateConfig({shardIteratorType: 'AT_TIMESTAMP', timestamp: 'aGreatTimestamp'});
      kcl.updateShardState({id: 'oneLastId', checkpoint: null});
      kcl.getIterator();
    };

    kcl.updateShardState({id: 'anotherId', checkpoint: 'aSequenceNumber'});
    kcl.getIterator();

  };
  const kcl = Kcl(config, dynoClient, kinesisClientTest);
  kcl.updateShardState({id: 'anId'});
  kcl.getIterator();
});

test('init, lease a shard', (t) => {
  t.plan(4);
  const config = {
    verbose: true,
    streamName: 'teststream',
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    },
    processRecords: function (records, cb) {
      console.log(`Processing ${records.length} records`);
      kcl.stop = true;
      clearTimeout(kcl.heartbeatTimeoutId);
      t.ok('processed records');
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  kcl.init(function (err, result) {
    t.equal(result.status, 'leased', 'shard is leased');
    t.true(result.expiresAt > +new Date(), 'expiry is in the future');
    setTimeout(function () {
      kinesisClient.putRecord({Data: 'hello', PartitionKey: 'a', StreamName: 'teststream'}, function (err) {
          t.error(err);
        }
      );
    }, 2000);
  });
});

test('checkpoint exposed function', (t) => {
  const config = {
    verbose: true,
    streamName: 'teststream',
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  kcl.init(function () {
    kcl.checkpoint('123', function () {
      sync.getShardList(function (err, shards) {
        t.equal(shards[1].checkpoint, '123', 'checkpoint is the same');
        kcl.stop = true;
        clearTimeout(kcl.heartbeatTimeoutId);
        t.end();
      })
    })
  });
});

test('backoff wait function, minimum and maximum', (t) => {
  const kcl = Kcl({}, dynoClient, kinesisClient);
  let result = 0;
  for (let i = 0; i < 100; i++) {
    let timer = kcl.getBackoffWait(1);
    if (timer < 1500) result++;
  }
  t.equal(result, 0, 'backoff time is never smaller than 1.5 seconds');
  for (let i = 0; i < 100; i++) {
    let timer = kcl.getBackoffWait(9);
    if (timer > 14000) result++;
  }
  t.equal(result, 0, 'backoff time is never bigger than 14 seconds');
  t.end();
});

test('heartbeat test', (t) => {
  const config = {
    verbose: true,
    streamName: 'teststream',
    abortKinesisRequestTimeout: 1000,
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.stop = true; // stop immediately
    kcl.init(function () { // need to init to get a shard id
      setTimeout(function () { // let things settle, we only want the heartbeat running
        kcl.stop = false;
        kcl.heartbeat(function (err, shardStatus) {
          let diff = +new Date() - 3000; // should of been updated in the last few seconds
          t.true(shardStatus.updated >= diff, 'was updated in the last few seconds');
          clearTimeout(kcl.heartbeatTimeoutId);

          // let's make sure the extending works properly. mock db.updateLease
          kcl.db.updateLease = function (shardId, cb) {
            console.log('calling fake leaseShard function');
            cb(null, {
              foo: 'bar'
            });
          };
          kcl.heartbeat(function (err, shardStatus) {
            t.true(shardStatus.foo, 'has foo property');
            t.equal(shardStatus.foo, 'bar', 'foo bar\'ed properly');
            clearTimeout(kcl.heartbeatTimeoutId);
            t.end();
          });
        })
      }, 1000);
    });
  }).catch(t.fail);
});

test('make sure heartbeat throws if we have a zombie (max processing time reached)', (t) => {
  t.plan(1);
  const config = {
    verbose: true,
    streamName: 'teststream',
    abortKinesisRequestTimeout: 1000,
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.stop = true; // stop immediately
    kcl.init(function () { // need to init to get a shard id
      setTimeout(function () { // let things settle, we only want the heartbeat running
        kcl.stop = false;
        kcl.shard.updated = +new Date() - (1000 * 60 * 5) - 1000; // more than 5 mins back
        t.throws(function () {
          kcl.heartbeat(function (err, shardStatus) {
            clearTimeout(kcl.heartbeatTimeoutId);
            t.ok('has thrown');
          })
        });
      }, 1000);
    });
  }).catch(t.fail);
});

test('teardown', util.teardown);