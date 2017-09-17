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

test('getRecords too many attempts throws', (t) => {
  const config = {
    verbose: true,
    streamName: 'teststream'
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  t.throws(function () {
    kcl.getRecords({}, 11, {}, function () {
    }, 'more than 10 attempts throws');
  });
  t.end();
});

test('getRecords request aborts if it takes too long', (t) => {
  t.plan(3);
  const abortKinesisRequestTimeout = 1000;
  const config = {
    verbose: true,
    streamName: 'teststream',
    abortKinesisRequestTimeout: abortKinesisRequestTimeout,
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    }
  };
  let startTime;
  const fakeKinesisClient = {
    getRecords: function (params) {
      console.log('getting records from fake client');
      return {
        abort: function () {
          const endTime = +new Date();
          const waitTime = endTime - startTime;
          console.log('calling abort from fake kinesis client');
          t.true(waitTime >= abortKinesisRequestTimeout, 'abort kicks in after 1 second'); // we abort requests that take more than X
          t.ok('called abort');
        },
        on: function () {
          // don't need to emit anything for this test
        },
        send: function () {
          startTime = +new Date();
          console.log('firing request from fake kinesis client');
          t.ok('called send');
        }
      }
    },
    getShardIterator: function (params, cb) {
      console.log('getting shard iterator from fake client');
      cb(null, {
        ShardIterator: '12345'
      })
    }
  };

  const kcl = Kcl(config, dynoClient, fakeKinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.init(function (err, result) {
      setTimeout(() => clearTimeout(kcl.heartbeatTimeoutId), 1000);
    });
  }).catch(t.fail);
});

test('getRecords errors retry', (t) => {
  t.plan(4);
  const config = {
    verbose: true,
    streamName: 'teststream',
    abortKinesisRequestTimeout: 1000,
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    }
  };

  const EventEmitter = require('events').EventEmitter;
  const fakeEventEmitter = new EventEmitter();
  let once = true;

  const fakeKinesisClient = {
    getRecords: function (params) {
      fakeEventEmitter.abort = function () {
        console.log('setting fake abort function on event emitter');
        t.ok('abort kicked in');
      };
      fakeEventEmitter.send = function () {
        t.ok('send function called');
        console.log('setting fake send function on event emitter');
        if (once) {
          once = false;
          fakeEventEmitter.emit('error', {
            code: 'SyntaxError'
          });
        }
      };
      return fakeEventEmitter;
    },
    getShardIterator: function (params, cb) {
      console.log('getting shard iterator from fake client');
      cb(null, {
        ShardIterator: '12345'
      })
    }
  };

  const kcl = Kcl(config, dynoClient, fakeKinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.init(function (err, result) {
      setTimeout(() => clearTimeout(kcl.heartbeatTimeoutId), 1000);
    });
  }).catch(t.fail);
});

test('getRecords is successful under normal conditions. Processing works as expected', (t) => {
  const config = {
    verbose: true,
    streamName: 'teststream',
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    },
    processRecords: function (records, cb) {
      console.log('Processing records');
      t.true(Array.isArray(records), 'records is an array');
      t.equal('hello', records[0].Data.toString(), 'value in record matches');
      kcl.stop = true;
      t.end();
      cb(null, null);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.init(function (err) {
      if (err) t.fail(err);
      setTimeout(() => clearTimeout(kcl.heartbeatTimeoutId), 1000);
      kinesisClient.putRecord({Data: 'hello', PartitionKey: 'a', StreamName: 'teststream'}, function (err, data) {
        t.error(err);
        t.deepEqual(Object.keys(data), ['ShardId', 'SequenceNumber'], 'returns shard id and sequence number');
      })
    });
  }).catch(t.fail);
});

test('checkpointing after processRecords works', (t) => {
  const config = {
    verbose: true,
    streamName: 'teststream',
    init: function (cb) {
      console.log('Init my test function');
      cb(null, null);
    },
    processRecords: function (records, cb) {
      console.log('Processing records');
      cb(null, true);
    }
  };
  const kcl = Kcl(config, dynoClient, kinesisClient);
  util.resetState(sync, dynoClient).then(function () {
    kcl.init(function (err) {
      if (err) t.fail(err);
      setTimeout(() => clearTimeout(kcl.heartbeatTimeoutId), 1000);
      let q = queue(1);
      q.defer(sync.getShardList);
      q.defer(kinesisClient.putRecord.bind(kinesisClient), {
        Data: 'hello',
        PartitionKey: 'a',
        StreamName: 'teststream'
      });
      q.defer(function (cb) {
        setTimeout(function () {
          cb();
        }, 4000); // this should be enough time to find our record and checkpoint
      });
      q.defer(sync.getShardList);
      q.awaitAll(function (err, data) {
        if (err) t.fail(err);
        t.true(!data[0][0].checkpoint, 'we have no checkpoint yet');
        t.true(data[3][0].checkpoint, 'we have a checkpoint now');
        kcl.stop = true;
        t.end();
      })
    });
  }).catch(t.fail);
});

test('teardown', util.teardown);