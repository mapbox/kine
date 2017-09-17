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

test('mark shard as complete', (t) => {
  process.exit = function (code) {
    sync.getShardList(function (err, result) {
      if (err) t.fail(err);
      t.equal(result[0].status, 'complete', 'shard marked as complete');
      t.end();
    })
  };
  util.resetState(sync, dynoClient).then(function () {
    const config = {
      verbose: true,
      streamName: 'teststream',
      init: function (cb) {
        console.log('Init my test function');
        cb(null, null);
      }
    };
    const kcl = Kcl(config, dynoClient, kinesisClient);
    kcl.init(function (err) {
      if (err) t.fail(err);
      kcl.stop = true;
      kcl.markShardAsComplete(kcl.shard);
    });
  }).catch(t.fail)
});

test('null sharditerator closes the shard', (t) => {
  t.plan(1);
  const config = {
    verbose: true,
    streamName: 'teststream',
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  kcl.markShardAsComplete = function () {
    console.log('Calling mock markShardAsComplete function');
    t.ok(1);
  };
  util.resetState(sync, dynoClient).then(function () {
    kcl.init(function () {
      kcl.stop = true;
      setTimeout(function () {
        kcl.checkpointSaved(kcl.shard, null);
      }, 2000); // give time for KCL to stop
    });
  });
});

test('successful getRecords – with records', (t) => {
  t.plan(1);
  let resp = {
    data: {
      Records: [1, 2, 3]
    }
  };
  const kcl = Kcl({}, dynoClient, kinesisClient);
  kcl.postProcessWithRecords = function (data) {
    console.log('calling with records');
    t.deepEqual(data, resp, 'response is passed along');
  };
  kcl.successfulGetRecords(resp, {}, null);
});

test('successful getRecords – without records', (t) => {
  t.plan(1);
  let resp = {
    data: {
      Records: []
    }
  };
  const kcl = Kcl({}, dynoClient, kinesisClient);
  kcl.postProcessWithoutRecords = function (data) {
    console.log('calling without records');
    t.deepEqual(data, resp, 'response is passed along');
  };
  kcl.successfulGetRecords(resp, {}, null);
});

test('nextShardIterator gets set properly', (t) => {
  t.plan(1);
  let resp = {
    data: {
      Records: [],
      NextShardIterator: 'my value'
    }
  };
  const kcl = Kcl({}, dynoClient, kinesisClient);
  kcl.postProcessWithoutRecords = function (data, iterator) {
    console.log('calling without records');
    t.equal(iterator, 'my value', 'shard iterator gets set');
  };
  kcl.successfulGetRecords(resp, {}, null);
});

test('shard is done, with onShardClosed function existing, and without.', (t) => {
  t.plan(2);
  const config = {
    onShardClosed: function (cb) {
      console.log('called onShardClosed function');
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  kcl.markShardAsComplete = function (shard) {
    console.log('calling fake markShardAsComplete function');
    t.deepEqual(shard, {aKey: 1}, 'value of shard is passed down');
  };
  let resp = { // mock no nextShardIterator, but a Records array
    data: {
      Records: []
    }
  };
  kcl.postProcessWithoutRecords(resp, null, {aKey: 1});
  kcl.updateConfig({}); //remove onShardClosed function
  kcl.postProcessWithoutRecords(resp, null, {aKey: 1}); // should still markAsComplete
});

test('put stats to CloudWatch (mocked)', (t) => {
  const config = {
    verbose: true,
    streamName: 'teststream',
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    },
    processRecords: function (records, cb) {
      console.log('Processing records');
      kcl.stop = true;
      cb(null, null);
    },
    cloudwatch: {
      putMetricData: function (params) {
        t.equal(params.MetricData[0].MetricName, 'ShardIteratorAgeInMs');
        t.equal(params.MetricData[0].Dimensions[0].Name, 'DeliveryStream');
        t.equal(params.MetricData[0].Dimensions[0].Value, 'teststream');
        t.true(params.MetricData[0].Value < 1000);
        return {
          on: function () {
          },
          send: function () {
            t.end();
          }
        }
      }
    },
    cloudwatchNamespace: 'aTestNamespace'
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.init(function (err) {
      if (err) t.fail(err);
      setTimeout(() => clearTimeout(kcl.heartbeatTimeoutId), 1000);
      kinesisClient.putRecord({Data: 'hello', PartitionKey: 'a', StreamName: 'teststream'}, function (err, data) {
      })
    });
  }).catch(t.fail);
});

test('teardown', util.teardown);